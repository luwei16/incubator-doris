#include "cloud/cloud_base_compaction.h"

#include "cloud/utils.h"
#include "common/config.h"
#include "gen_cpp/selectdb_cloud.pb.h"
#include "util/trace.h"
#include "util/uuid_generator.h"

namespace doris {

CloudBaseCompaction::CloudBaseCompaction(TabletSharedPtr tablet)
        : BaseCompaction(std::move(tablet)) {
    auto uuid = UUIDGenerator::instance()->next_uuid();
    std::stringstream ss;
    ss << uuid;
    _uuid = ss.str();
}

CloudBaseCompaction::~CloudBaseCompaction() = default;

Status CloudBaseCompaction::prepare_compact() {
    std::unique_lock lock(_tablet->get_base_compaction_lock(), std::try_to_lock);
    if (!lock.owns_lock()) {
        return Status::OLAPInternalError(OLAP_ERR_BE_TRY_BE_LOCK_ERROR);
    }
    TRACE("got base compaction lock");

    RETURN_IF_ERROR(_tablet->cloud_sync_rowsets());

    // MUST get compaction_cnt before `pick_rowsets_to_compact` to ensure statistic of tablet during `pick_rowsets_to_compact`
    // not lag behind start tablet job request.
    int64_t base_compaction_cnt = _tablet->base_compaction_cnt();
    int64_t cumulative_compaction_cnt = _tablet->cumulative_compaction_cnt();

    RETURN_IF_ERROR(pick_rowsets_to_compact());
    TRACE("rowsets picked");
    TRACE_COUNTER_INCREMENT("input_rowsets_count", _input_rowsets.size());
    VLOG_CRITICAL << "compaction range=[" << _input_rowsets.front()->start_version() << '-'
                  << _input_rowsets.back()->end_version() << ']';

    // prepare compaction job
    selectdb::TabletJobInfoPB job;
    job.set_id(_uuid);
    auto idx = job.mutable_idx();
    idx->set_tablet_id(_tablet->tablet_id());
    idx->set_table_id(_tablet->table_id());
    idx->set_index_id(_tablet->index_id());
    idx->set_partition_id(_tablet->partition_id());
    auto compaction_job = job.mutable_compaction();
    compaction_job->set_initiator(BackendOptions::get_localhost() + ':' +
                                  std::to_string(config::heartbeat_service_port));
    compaction_job->set_type(selectdb::TabletCompactionJobPB::BASE);
    compaction_job->set_base_compaction_cnt(base_compaction_cnt);
    compaction_job->set_cumulative_compaction_cnt(cumulative_compaction_cnt);
    return cloud::meta_mgr()->prepare_tablet_job(job);
}

Status CloudBaseCompaction::execute_compact_impl() {
    if (config::enable_base_compaction_idle_sched) {
        Thread::set_idle_sched();
    }
    std::unique_lock lock(_tablet->get_base_compaction_lock(), std::try_to_lock);
    if (!lock.owns_lock()) {
        return Status::OLAPInternalError(OLAP_ERR_BE_TRY_BE_LOCK_ERROR);
    }
    TRACE("got base compaction lock");

    SCOPED_ATTACH_TASK(_mem_tracker, ThreadContext::TaskType::COMPACTION);

    int64_t permits = get_compaction_permits();
    RETURN_NOT_OK(do_compaction(permits));
    TRACE("compaction finished");

    _state = CompactionState::SUCCESS;

    DorisMetrics::instance()->base_compaction_deltas_total->increment(_input_rowsets.size());
    DorisMetrics::instance()->base_compaction_bytes_total->increment(_input_rowsets_size);
    TRACE("save base compaction metrics");

    return Status::OK();
}

Status CloudBaseCompaction::update_tablet_meta() {
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
    job.set_id(_uuid);
    auto idx = job.mutable_idx();
    idx->set_tablet_id(_tablet->tablet_id());
    idx->set_table_id(_tablet->table_id());
    idx->set_index_id(_tablet->index_id());
    idx->set_partition_id(_tablet->partition_id());
    auto compaction_job = job.mutable_compaction();
    compaction_job->set_initiator(BackendOptions::get_localhost() + ':' +
                                  std::to_string(config::heartbeat_service_port));
    compaction_job->set_type(selectdb::TabletCompactionJobPB::BASE);
    compaction_job->set_input_cumulative_point(_tablet->cumulative_layer_point());
    compaction_job->set_output_cumulative_point(_tablet->cumulative_layer_point());
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

    int64_t base_compaction_cnt = _tablet->base_compaction_cnt();
    selectdb::TabletStatsPB stats;
    RETURN_IF_ERROR(cloud::meta_mgr()->commit_tablet_job(job, &stats));

    {
        std::lock_guard wrlock(_tablet->get_header_lock());
        if (_tablet->base_compaction_cnt() > base_compaction_cnt) {
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

void CloudBaseCompaction::garbage_collection() {
    selectdb::TabletJobInfoPB job;
    job.set_id(_uuid);
    auto idx = job.mutable_idx();
    idx->set_tablet_id(_tablet->tablet_id());
    idx->set_table_id(_tablet->table_id());
    idx->set_index_id(_tablet->index_id());
    idx->set_partition_id(_tablet->partition_id());
    auto compaction_job = job.mutable_compaction();
    compaction_job->set_initiator(BackendOptions::get_localhost() + ':' +
                                  std::to_string(config::heartbeat_service_port));
    compaction_job->set_type(selectdb::TabletCompactionJobPB::BASE);
    auto st = cloud::meta_mgr()->abort_tablet_job(job);
    if (!st.ok()) {
        LOG_WARNING("failed to gc compaction job").tag("tablet_id", _tablet->tablet_id()).error(st);
    }
}

} // namespace doris
