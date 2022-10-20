#include "cloud/cloud_cumulative_compaction.h"

#include "cloud/utils.h"
#include "common/config.h"
#include "common/logging.h"
#include "gen_cpp/selectdb_cloud.pb.h"
#include "util/trace.h"
#include "util/uuid_generator.h"

namespace doris {

CloudCumulativeCompaction::CloudCumulativeCompaction(TabletSharedPtr tablet)
        : CumulativeCompaction(std::move(tablet)) {
    auto uuid = UUIDGenerator::instance()->next_uuid();
    std::stringstream ss;
    ss << uuid;
    _uuid = ss.str();
}

CloudCumulativeCompaction::~CloudCumulativeCompaction() = default;

Status CloudCumulativeCompaction::prepare_compact() {
    if (_tablet->tablet_state() != TABLET_RUNNING) {
        return Status::InternalError("invalid tablet state. tablet_id={}", _tablet->tablet_id());
    }
    std::unique_lock lock(_tablet->get_cumulative_compaction_lock(), std::try_to_lock);
    if (!lock.owns_lock()) {
        return Status::OLAPInternalError(OLAP_ERR_BE_TRY_BE_LOCK_ERROR);
    }
    TRACE("got cumulative compaction lock");

    RETURN_IF_ERROR(_tablet->cloud_sync_rowsets());

    // MUST get compaction_cnt before `pick_rowsets_to_compact` to ensure statistic of tablet during `pick_rowsets_to_compact`
    // not lag behind start tablet job request.
    int64_t base_compaction_cnt = _tablet->base_compaction_cnt();
    int64_t cumulative_compaction_cnt = _tablet->cumulative_compaction_cnt();

    // pick rowsets to compact
    auto st = pick_rowsets_to_compact();
    if (!st.ok()) {
        if (_last_delete_version.first != -1) {
            // we meet a delete version, should increase the cumulative point to let base compaction handle the delete version.
            // plus 1 to skip the delete version.
            // NOTICE: after that, the cumulative point may be larger than max version of this tablet, but it doesn't matter.
            update_cumulative_point(base_compaction_cnt, cumulative_compaction_cnt);
        }
        return st;
    }
    TRACE("rowsets picked");
    TRACE_COUNTER_INCREMENT("input_rowsets_count", _input_rowsets.size());
    VLOG_CRITICAL << "compaction range=[" << _input_rowsets.front()->start_version() << '-'
                  << _input_rowsets.back()->end_version() << ']';

    // prepare compaction job
    selectdb::TabletJobInfoPB job;
    auto idx = job.mutable_idx();
    idx->set_tablet_id(_tablet->tablet_id());
    idx->set_table_id(_tablet->table_id());
    idx->set_index_id(_tablet->index_id());
    idx->set_partition_id(_tablet->partition_id());
    auto compaction_job = job.mutable_compaction();
    compaction_job->set_id(_uuid);
    compaction_job->set_initiator(BackendOptions::get_localhost() + ':' +
                                  std::to_string(config::heartbeat_service_port));
    compaction_job->set_type(selectdb::TabletCompactionJobPB::CUMULATIVE);
    compaction_job->set_base_compaction_cnt(base_compaction_cnt);
    compaction_job->set_cumulative_compaction_cnt(cumulative_compaction_cnt);
    using namespace std::chrono;
    int64_t expiration =
            duration_cast<seconds>(system_clock::now().time_since_epoch()).count() + 21600; // 6h
    compaction_job->set_expiration(expiration); // FIXME(cyx): estimate according to data size
    return cloud::meta_mgr()->prepare_tablet_job(job);
}

Status CloudCumulativeCompaction::execute_compact_impl() {
    std::unique_lock lock(_tablet->get_cumulative_compaction_lock(), std::try_to_lock);
    if (!lock.owns_lock()) {
        return Status::OLAPInternalError(OLAP_ERR_BE_TRY_BE_LOCK_ERROR);
    }
    TRACE("got cumulative compaction lock");

    int64_t permits = get_compaction_permits();
    RETURN_NOT_OK(do_compaction(permits));
    TRACE("compaction finished");

    _state = CompactionState::SUCCESS;

    DorisMetrics::instance()->cumulative_compaction_deltas_total->increment(_input_rowsets.size());
    DorisMetrics::instance()->cumulative_compaction_bytes_total->increment(_input_rowsets_size);
    TRACE("save cumulative compaction metrics");

    return Status::OK();
}

Status CloudCumulativeCompaction::update_tablet_meta() {
    // calculate new cumulative point
    int64_t new_cumulative_point = _tablet->cumulative_compaction_policy()->new_cumulative_point(
            _input_rowsets, _output_rowset, _last_delete_version);
    // commit compaction job
    int64_t input_rows = 0;
    int64_t input_segments = 0;
    int64_t input_data_size = 0;
    for (auto& rs : _input_rowsets) {
        input_rows += rs->num_rows();
        input_segments += rs->num_segments();
        input_data_size += rs->data_disk_size();
    }
    selectdb::TabletJobInfoPB job;
    auto idx = job.mutable_idx();
    idx->set_tablet_id(_tablet->tablet_id());
    idx->set_table_id(_tablet->table_id());
    idx->set_index_id(_tablet->index_id());
    idx->set_partition_id(_tablet->partition_id());
    auto compaction_job = job.mutable_compaction();
    compaction_job->set_id(_uuid);
    compaction_job->set_initiator(BackendOptions::get_localhost() + ':' +
                                  std::to_string(config::heartbeat_service_port));
    compaction_job->set_type(selectdb::TabletCompactionJobPB::CUMULATIVE);
    compaction_job->set_input_cumulative_point(_tablet->cumulative_layer_point());
    compaction_job->set_output_cumulative_point(new_cumulative_point);
    compaction_job->set_num_input_rows(input_rows);
    compaction_job->set_num_output_rows(_output_rowset->num_rows());
    compaction_job->set_size_input_rowsets(input_data_size);
    compaction_job->set_size_output_rowsets(_output_rowset->data_disk_size());
    compaction_job->set_num_input_segments(input_segments);
    compaction_job->set_num_output_segments(_output_rowset->num_segments());
    compaction_job->set_num_input_rowsets(_input_rowsets.size());
    compaction_job->set_num_output_rowsets(1);
    compaction_job->add_input_versions(_input_rowsets.front()->start_version());
    compaction_job->add_input_versions(_input_rowsets.back()->end_version());
    compaction_job->add_output_versions(_output_rowset->end_version());
    compaction_job->set_txn_id(_output_rowset->txn_id());
    compaction_job->add_output_rowset_ids(_output_rowset->rowset_id().to_string());

    int64_t cumulative_compaction_cnt = _tablet->cumulative_compaction_cnt();
    selectdb::TabletStatsPB stats;
    RETURN_IF_ERROR(cloud::meta_mgr()->commit_tablet_job(job, &stats));

    {
        std::lock_guard wrlock(_tablet->get_header_lock());
        if (_tablet->cumulative_compaction_cnt() > cumulative_compaction_cnt) {
            // This could happen while calling `sync_tablet_rowsets` during `commit_tablet_job`
            return Status::OK();
        }
        _tablet->cloud_delete_rowsets(_input_rowsets);
        _tablet->cloud_add_rowsets({_output_rowset}, false);
        _tablet->set_base_compaction_cnt(stats.base_compaction_cnt());
        _tablet->set_cumulative_compaction_cnt(stats.cumulative_compaction_cnt());
        _tablet->set_cumulative_layer_point(stats.cumulative_point());
        _tablet->reset_approximate_stats(stats.num_rowsets(), stats.num_segments(),
                                         stats.num_rows(), stats.data_size());
    }
    return Status::OK();
}

void CloudCumulativeCompaction::garbage_collection() {
    selectdb::TabletJobInfoPB job;
    auto idx = job.mutable_idx();
    idx->set_tablet_id(_tablet->tablet_id());
    idx->set_table_id(_tablet->table_id());
    idx->set_index_id(_tablet->index_id());
    idx->set_partition_id(_tablet->partition_id());
    auto compaction_job = job.mutable_compaction();
    compaction_job->set_id(_uuid);
    compaction_job->set_initiator(BackendOptions::get_localhost() + ':' +
                                  std::to_string(config::heartbeat_service_port));
    compaction_job->set_type(selectdb::TabletCompactionJobPB::CUMULATIVE);
    auto st = cloud::meta_mgr()->abort_tablet_job(job);
    if (!st.ok()) {
        LOG_WARNING("failed to gc compaction job").tag("tablet_id", _tablet->tablet_id()).error(st);
    }
}

Status CloudCumulativeCompaction::pick_rowsets_to_compact() {
    std::vector<RowsetSharedPtr> candidate_rowsets;
    _tablet->pick_candidate_rowsets_to_cumulative_compaction(&candidate_rowsets);

    if (candidate_rowsets.empty()) {
        return Status::OLAPInternalError(OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSION);
    }

    // candidate_rowsets may not be continuous
    // So we need to choose the longest continuous path from it.
    std::vector<Version> missing_versions;
    RETURN_NOT_OK(find_longest_consecutive_version(&candidate_rowsets, &missing_versions));
    if (!missing_versions.empty()) {
        DCHECK(missing_versions.size() == 2);
        LOG(WARNING) << "There are missed versions among rowsets. "
                     << "prev rowset verison=" << missing_versions[0]
                     << ", next rowset version=" << missing_versions[1]
                     << ", tablet=" << _tablet->full_name();
    }

    size_t compaction_score = 0;
    _tablet->cumulative_compaction_policy()->pick_input_rowsets(
            _tablet.get(), candidate_rowsets,
            config::max_cumulative_compaction_num_singleton_deltas,
            config::min_cumulative_compaction_num_singleton_deltas, &_input_rowsets,
            &_last_delete_version, &compaction_score);

    if (_input_rowsets.empty()) {
        return Status::OLAPInternalError(OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSION);
    } else if (_input_rowsets.size() == 1 &&
               !_input_rowsets.front()->rowset_meta()->is_segments_overlapping()) {
        VLOG_DEBUG << "there is only one rowset and not overlapping. tablet_id="
                   << _tablet->tablet_id() << ", version=" << _input_rowsets.front()->version();
        return Status::OLAPInternalError(OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSION);
    }
    return Status::OK();
}

void CloudCumulativeCompaction::update_cumulative_point(int64_t base_compaction_cnt,
                                                        int64_t cumulative_compaction_cnt) {
    selectdb::TabletJobInfoPB job;
    auto idx = job.mutable_idx();
    idx->set_tablet_id(_tablet->tablet_id());
    idx->set_table_id(_tablet->table_id());
    idx->set_index_id(_tablet->index_id());
    idx->set_partition_id(_tablet->partition_id());
    auto compaction_job = job.mutable_compaction();
    compaction_job->set_id(_uuid);
    compaction_job->set_initiator(BackendOptions::get_localhost() + ':' +
                                  std::to_string(config::heartbeat_service_port));
    compaction_job->set_type(selectdb::TabletCompactionJobPB::EMPTY_CUMULATIVE);
    compaction_job->set_base_compaction_cnt(base_compaction_cnt);
    compaction_job->set_cumulative_compaction_cnt(cumulative_compaction_cnt);
    auto st = cloud::meta_mgr()->prepare_tablet_job(job);
    if (!st.ok()) {
        LOG_WARNING("failed to update cumulative point to meta srv")
                .tag("tablet_id", _tablet->tablet_id())
                .error(st);
    }
    int64_t input_cumulative_point = _tablet->cumulative_layer_point();
    int64_t output_cumulative_point = _last_delete_version.first + 1;
    compaction_job->set_input_cumulative_point(input_cumulative_point);
    compaction_job->set_output_cumulative_point(output_cumulative_point);
    selectdb::TabletStatsPB stats;
    st = cloud::meta_mgr()->commit_tablet_job(job, &stats);
    if (!st.ok()) {
        LOG_WARNING("failed to update cumulative point to meta srv")
                .tag("tablet_id", _tablet->tablet_id())
                .error(st);
        return;
    }
    LOG_INFO("do empty cumulative compaction to update cumulative point")
            .tag("tablet_id", _tablet->tablet_id())
            .tag("input_cumulative_point", input_cumulative_point)
            .tag("output_cumulative_point", output_cumulative_point);
    {
        std::lock_guard wrlock(_tablet->get_header_lock());
        if (_tablet->cumulative_compaction_cnt() > cumulative_compaction_cnt) {
            // This could happen while calling `sync_tablet_rowsets` during `commit_tablet_job`
            return;
        }
        _tablet->set_base_compaction_cnt(stats.base_compaction_cnt());
        _tablet->set_cumulative_compaction_cnt(stats.cumulative_compaction_cnt());
        _tablet->set_cumulative_layer_point(stats.cumulative_point());
        _tablet->reset_approximate_stats(stats.num_rowsets(), stats.num_segments(),
                                         stats.num_rows(), stats.data_size());
    }
}

} // namespace doris
