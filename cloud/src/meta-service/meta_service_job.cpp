
// clang-format off
#include "meta_service.h"

#include "meta-service/keys.h"
#include "common/logging.h"
#include "common/util.h"
#include "common/sync_point.h"

#include "brpc/closure_guard.h"
#include "brpc/controller.h"

#include <chrono>
// clang-format on

#define RPC_PREPROCESS()                                                                        \
    auto ctrl = static_cast<brpc::Controller*>(controller);                                     \
    std::string rpc_name(__PRETTY_FUNCTION__);                                                  \
    rpc_name = rpc_name.substr(0, rpc_name.find('(')) + "()";                                   \
    LOG(INFO) << "begin " << rpc_name << " rpc from " << ctrl->remote_side()                    \
              << " request=" << request->DebugString();                                         \
    brpc::ClosureGuard closure_guard(done);                                                     \
    [[maybe_unused]] int ret = 0;                                                               \
    [[maybe_unused]] std::stringstream ss;                                                      \
    [[maybe_unused]] MetaServiceCode code = MetaServiceCode::OK;                                \
    [[maybe_unused]] std::string msg = "OK";                                                    \
    std::unique_ptr<int, std::function<void(int*)>> defer_status((int*)0x01, [&](int*) {        \
        response->mutable_status()->set_code(code);                                             \
        response->mutable_status()->set_msg(msg);                                               \
        LOG(INFO) << "finish " << rpc_name << " from " << ctrl->remote_side() << " ret=" << ret \
                  << " code=" << code << " msg=\"" << msg << "\""                               \
                  << " response=" << response->DebugString();                                   \
    });

// Empty string not is not processed
template <typename T, size_t S>
static inline constexpr size_t get_file_name_offset(const T (&s)[S], size_t i = S - 1) {
    return (s[i] == '/' || s[i] == '\\') ? i + 1 : (i > 0 ? get_file_name_offset(s, i - 1) : 0);
}
#define SS (ss << &__FILE__[get_file_name_offset(__FILE__)] << ":" << __LINE__ << " ")

namespace selectdb {

extern std::string get_instance_id(const std::shared_ptr<ResourceManager>& rc_mgr,
                                   const std::string& cloud_unique_id);

/**
 * 1. Check existing job
 * 2. Check expiration
 * 3. Put a new job
 */
void MetaServiceImpl::start_tablet_job(::google::protobuf::RpcController* controller,
                                       const ::selectdb::StartTabletJobRequest* request,
                                       ::selectdb::StartTabletJobResponse* response,
                                       ::google::protobuf::Closure* done) {
    RPC_PREPROCESS();
    std::string cloud_unique_id = request->cloud_unique_id();
    std::string instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        SS << "cannot find instance_id with cloud_unique_id="
           << (cloud_unique_id.empty() ? "(empty)" : cloud_unique_id);
        msg = ss.str();
        return;
    }

    if (!request->has_job() ||
        (!request->job().has_compaction() && !request->job().has_schema_change())) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no valid job specified";
        return;
    }

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }

    int64_t tablet_id =
            request->job().idx().has_tablet_id() ? request->job().idx().tablet_id() : -1;
    if (tablet_id <= 0) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no valid tablet_id given";
        return;
    }
    int64_t table_id = request->job().idx().table_id();
    int64_t index_id = request->job().idx().index_id();
    int64_t partition_id = request->job().idx().partition_id();
    // Get tablet id index from kv
    if (table_id <= 0 || index_id <= 0 || partition_id <= 0) {
        std::string idx_key = meta_tablet_idx_key({instance_id, tablet_id});
        std::string idx_val;
        ret = txn->get(idx_key, &idx_val);
        LOG(INFO) << "get tablet meta, tablet_id=" << tablet_id << " key=" << hex(idx_key);
        if (ret != 0) {
            std::stringstream ss;
            SS << "failed to get table id with tablet_id from kv, err="
               << (ret == 1 ? "not found" : "internal error");
            code = ret == 1 ? MetaServiceCode::UNDEFINED_ERR : MetaServiceCode::KV_TXN_GET_ERR;
            msg = ss.str();
            return;
        }
        TabletIndexPB idx_pb;
        if (!idx_pb.ParseFromString(idx_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = "malformed tablet table value";
            return;
        }
        table_id = idx_pb.table_id();
        index_id = idx_pb.index_id();
        partition_id = idx_pb.partition_id();
        if (tablet_id != idx_pb.tablet_id()) {
            code = MetaServiceCode::UNDEFINED_ERR;
            msg = "internal error";
            LOG(WARNING) << "unexpected error given_tablet_id=" << tablet_id
                         << " idx_pb_tablet_id=" << idx_pb.tablet_id();
            return;
        }
    }

    // Begin to process start tablet job

    std::string job_key =
            job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    std::string job_val;
    TabletJobInfoPB job_pb;
    ret = txn->get(job_key, &job_val);

    TEST_SYNC_POINT_CALLBACK("start_tablet_job_get_key_ret", &ret);

    if (ret == 0) {
        // TODO(gavin): check expiration
        job_pb.ParseFromString(job_val);
        job_pb.has_compaction();
        SS << "job already started instance_id=" << instance_id << " tablet_id=" << tablet_id
           << " job=" << proto_to_json(job_pb);
        msg = ss.str();
        code = MetaServiceCode::UNDEFINED_ERR;
        return;
    }

    if (ret != 1) {
        SS << "failed to get tablet job, instance_id=" << instance_id << " tablet_id=" << tablet_id
           << " key=" << hex(job_key) << " ret=" << ret;
        msg = ss.str();
        code = MetaServiceCode::KV_TXN_GET_ERR;
        return;
    }

    job_pb.CopyFrom(request->job());

    // TODO(gavin): check arguments
    LOG(INFO) << (job_pb.has_compaction()      ? "compaction"
                  : job_pb.has_schema_change() ? "schema_change"
                                               : "")
              << " job to save, instance_id=" << instance_id << " tablet_id=" << tablet_id
              << " job=" << proto_to_json(request->job().compaction());

    job_val = job_pb.SerializeAsString();
    if (job_val.empty()) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        msg = "pb serialization error";
        return;
    }

    txn->put(job_key, job_val);
    LOG(INFO) << "put tablet job key=" << hex(job_key);

    ret = txn->commit();
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = "failed to commit job info";
        return;
    }

    txn_kv_->create_txn(&txn);
    job_val.clear();
    ret = txn->get(job_key, &job_val);
    LOG(INFO) << "xxx ret=" << ret << " sizeof_job_val=" << job_val.size();
}

void process_compaction_job(MetaServiceCode& code, std::string& msg, std::stringstream& ss,
                            int& ret, std::unique_ptr<Transaction>& txn,
                            const ::selectdb::FinishTabletJobRequest* request,
                            TabletJobInfoPB& recorded_job, std::string& instance_id,
                            int64_t& table_id, int64_t& index_id, int64_t& partition_id,
                            int64_t& tablet_id, std::string& job_key, int64_t& now,
                            bool& need_commit) {
    // process compaction
    if (!recorded_job.has_compaction()) {
        SS << "there is no running compaction, tablet_id=" << tablet_id;
        msg = ss.str();
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }

    if (request->action() != FinishTabletJobRequest::COMMIT) {
        SS << "unsupported action, only commit is supported by now, tablet_id=" << tablet_id
           << " action=" << request->action();
        msg = ss.str();
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }

    // TODO(gavin): more check
    auto& compaction = request->job().compaction();

    auto tablet_key = meta_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    std::string tablet_val;
    ret = txn->get(tablet_key, &tablet_val);
    LOG(INFO) << "get tablet meta, tablet_id=" << tablet_id << " key=" << hex(tablet_key);
    if (ret != 0) {
        SS << "failed to get tablet meta, tablet_id=" << tablet_id << " key=" << hex(tablet_key)
           << " ret=" << ret;
        code = ret == 1 ? MetaServiceCode::UNDEFINED_ERR : MetaServiceCode::KV_TXN_GET_ERR;
        msg = ss.str();
        return;
    }
    doris::TabletMetaPB tablet_meta;
    tablet_meta.ParseFromString(tablet_val);
    tablet_meta.set_cumulative_layer_point(compaction.output_cumulative_point());
    tablet_val = tablet_meta.SerializeAsString();
    DCHECK(!tablet_val.empty());
    txn->put(tablet_key, tablet_val);
    LOG(INFO) << "update tablet meta, tablet_id=" << tablet_id
              << " cumulative_point=" << compaction.output_cumulative_point()
              << " key=" << hex(tablet_key);

    auto stats_key = stats_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    std::string stats_val;
    ret = txn->get(stats_key, &stats_val);
    LOG(INFO) << "get tablet stats, tablet_id=" << tablet_id << " key=" << hex(stats_key)
              << " ret=" << ret;
    if (ret != 0) {
        code = ret == 1 ? MetaServiceCode::UNDEFINED_ERR : MetaServiceCode::KV_TXN_GET_ERR;
        SS << (ret == 1 ? "internal error" : "get kv error") << " tablet_id=" << tablet_id
           << " key=" << hex(stats_key) << " ret=" << ret;
        msg = ss.str();
        return;
    }
    // Update tablet stats
    TabletStatsPB stats;
    stats.ParseFromString(stats_val);
    // clang-format off
    stats.set_base_compaction_cnt(stats.base_compaction_cnt() + (compaction.type() == TabletCompactionJobPB::BASE));
    stats.set_cumulative_compaction_cnt(stats.cumulative_compaction_cnt() + (compaction.type() == TabletCompactionJobPB::CUMULATIVE));
    stats.set_cumulative_point(compaction.output_cumulative_point());
    stats.set_num_rows(stats.num_rows() + (compaction.num_output_rows() - compaction.num_input_rows()));
    stats.set_data_size(stats.data_size() + (compaction.size_output_rowsets() - compaction.size_input_rowsets()));
    stats.set_num_rowsets(stats.num_rowsets() + (compaction.num_output_rowsets() - compaction.num_input_rowsets()));
    stats.set_num_segments(stats.num_segments() + (compaction.num_output_segments() - compaction.num_input_segments()));
    stats.set_last_compaction_time(now);
    // clang-format on

    stats_val = stats.SerializeAsString();
    DCHECK(!stats_val.empty());
    txn->put(stats_key, stats_val);
    LOG(INFO) << "update tablet stats tabelt_id=" << tablet_id << " key=" << hex(stats_key);

    // TODO(gavin): move deleted job info into recycle or history
    txn->remove(job_key);
    LOG(INFO) << "remove tablet job tabelt_id=" << tablet_id << " key=" << hex(job_key);

    need_commit = true;
}

void MetaServiceImpl::finish_tablet_job(::google::protobuf::RpcController* controller,
                                        const ::selectdb::FinishTabletJobRequest* request,
                                        ::selectdb::FinishTabletJobResponse* response,
                                        ::google::protobuf::Closure* done) {
    RPC_PREPROCESS();
    std::string cloud_unique_id = request->cloud_unique_id();
    std::string instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        SS << "cannot find instance_id with cloud_unique_id="
           << (cloud_unique_id.empty() ? "(empty)" : cloud_unique_id);
        msg = ss.str();
        LOG(INFO) << msg;
        return;
    }

    if (!request->has_job() ||
        (!request->job().has_compaction() && !request->job().has_schema_change())) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no valid job specified";
        return;
    }

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }

    int64_t tablet_id =
            request->job().idx().has_tablet_id() ? request->job().idx().tablet_id() : -1;
    if (tablet_id <= 0) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no valid tablet_id given";
        return;
    }
    int64_t table_id = request->job().idx().table_id();
    int64_t index_id = request->job().idx().index_id();
    int64_t partition_id = request->job().idx().partition_id();
    // Get tablet id index from kv
    if (table_id <= 0 || index_id <= 0 || partition_id <= 0) {
        std::string idx_key = meta_tablet_idx_key({instance_id, tablet_id});
        std::string idx_val;
        ret = txn->get(idx_key, &idx_val);
        LOG(INFO) << "get tablet meta, tablet_id=" << tablet_id << " key=" << hex(idx_key);
        if (ret != 0) {
            std::stringstream ss;
            SS << "failed to get table id from tablet_id, err="
               << (ret == 1 ? "not found" : "internal error");
            code = MetaServiceCode::KV_TXN_GET_ERR;
            msg = ss.str();
            return;
        }
        TabletIndexPB idx_pb;
        if (!idx_pb.ParseFromString(idx_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = "malformed tablet table value";
            return;
        }
        table_id = idx_pb.table_id();
        index_id = idx_pb.index_id();
        partition_id = idx_pb.partition_id();
        if (tablet_id != idx_pb.tablet_id()) {
            code = MetaServiceCode::UNDEFINED_ERR;
            msg = "internal error";
            LOG(WARNING) << "unexpected error given_tablet_id=" << tablet_id
                         << " idx_pb_tablet_id=" << idx_pb.tablet_id();
            return;
        }
    }

    // TODO(gaivn): remove duplicated code with start_tablet_job()
    // Begin to process finish tablet job

    std::string job_key =
            job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    std::string job_val;
    ret = txn->get(job_key, &job_val);
    LOG(INFO) << "get job ret=" << ret << " key=" << hex(job_key);
    if (ret != 0) {
        SS << (ret == 1 ? "job not found," : "internal error,") << " instance_id=" << instance_id
           << " tablet_id=" << tablet_id << " job=" << proto_to_json(request->job());
        msg = ss.str();
        code = ret == 1 ? MetaServiceCode::INVALID_ARGUMENT : MetaServiceCode::KV_TXN_GET_ERR;
        return;
    }
    TabletJobInfoPB recorded_job;
    recorded_job.ParseFromString(job_val);

    using namespace std::chrono;
    int64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    if (recorded_job.expiration() > 0 && recorded_job.expiration() < now) {
        code = MetaServiceCode::EXPIRED_JOB;
        SS << "expired compaction job, tablet_id=" << tablet_id
           << " job=" << proto_to_json(recorded_job);
        msg = ss.str();
        // txn->remove(job_key);
        // LOG(INFO) << "remove expired job, tablet_id=" << tablet_id << " key=" << hex(job_key);
        return;
    }

    if (request->job().id() != recorded_job.id()) {
        SS << "unmatched job id, recorded_id=" << recorded_job.id()
           << " given_id=" << request->job().id()
           << " recorded_job=" << proto_to_json(recorded_job.compaction())
           << " given_job=" << proto_to_json(request->job());
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = ss.str();
        return;
    }

    bool need_commit = false;
    std::unique_ptr<int, std::function<void(int*)>> defer_commit(
            (int*)0x01, [&ret, &txn, &code, &msg, &need_commit](int*) {
                if (!need_commit) return;
                ret = txn->commit();
                if (ret != 0) {
                    code = MetaServiceCode::KV_TXN_COMMIT_ERR;
                    msg = "failed to commit job info";
                    return;
                }
            });

    if (request->job().has_compaction()) {
        process_compaction_job(code, msg, ss, ret, txn, request, recorded_job, instance_id,
                               table_id, index_id, partition_id, tablet_id, job_key, now,
                               need_commit);
        return;
    }

    if (request->job().has_compaction()) { // process schema change
    }
}

#undef RPC_PREPROCESS
#undef SS
} // namespace selectdb
// vim: et tw=100 ts=4 sw=4 cc=80:
