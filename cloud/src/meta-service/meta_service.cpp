
// clang-format off
#include "meta_service.h"
#include <gen_cpp/selectdb_cloud.pb.h>
#include "meta-service/doris_txn.h"
#include "meta-service/keys.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/util.h"
#include "common/sync_point.h"

#include "brpc/channel.h"
#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "bthread/bthread.h"
#include "google/protobuf/util/json_util.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/schema.h"

#include <chrono>
#include <iomanip>
#include <ios>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>
// clang-format on

using namespace std::chrono;

namespace selectdb {

/**
 * Nodtifies other metaservice to refresh instance
 */
void notify_refresh_instance(std::shared_ptr<TxnKv> txn_kv, const std::string& instance_id);

static void* run_bthread_work(void* arg) {
    auto f = reinterpret_cast<std::function<void()>*>(arg);
    (*f)();
    delete f;
    return nullptr;
}

MetaServiceImpl::MetaServiceImpl(std::shared_ptr<TxnKv> txn_kv,
                                 std::shared_ptr<ResourceManager> resource_mgr) {
    txn_kv_ = txn_kv;
    resource_mgr_ = resource_mgr;
}

MetaServiceImpl::~MetaServiceImpl() {}

std::string static trim(std::string& str) {
    const std::string drop = "/ \t";
    str.erase(str.find_last_not_of(drop) + 1);
    return str.erase(0, str.find_first_not_of(drop));
}

// FIXME(gavin): should it be a member function of ResourceManager?
std::string get_instance_id(const std::shared_ptr<ResourceManager>& rc_mgr,
                            const std::string& cloud_unique_id) {
    {
        [[maybe_unused]] std::string tmp_ret;
        TEST_SYNC_POINT_RETURN_WITH_VALUE("get_instance_id", &tmp_ret);
    }

    std::vector<NodeInfo> nodes;
    std::string err = rc_mgr->get_node(cloud_unique_id, &nodes);
    if (!err.empty()) {
        LOG(INFO) << "failed to check instance info, err=" << err;
        return "";
    }

    std::string instance_id;
    for (auto& i : nodes) {
        if (!instance_id.empty() && instance_id != i.instance_id) {
            LOG(WARNING) << "cloud_unique_id is one-to-many instance_id, "
                         << " cloud_unique_id=" << cloud_unique_id
                         << " current_instance_id=" << instance_id
                         << " later_instance_id=" << i.instance_id;
        }
        instance_id = i.instance_id; // The last wins
    }
    return instance_id;
}

//TODO: we need move begin/commit etc txn to TxnManager
void MetaServiceImpl::begin_txn(::google::protobuf::RpcController* controller,
                                const ::selectdb::BeginTxnRequest* request,
                                ::selectdb::BeginTxnResponse* response,
                                ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << __PRETTY_FUNCTION__ << " rpc from " << ctrl->remote_side() << " request:\n"
              << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    [[maybe_unused]] std::stringstream ss;

    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&code, &msg, &response, &ctrl](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << __PRETTY_FUNCTION__ << " finish " << ctrl->remote_side()
                          << " response:\n"
                          << response->DebugString();
            });

    if (!request->has_txn_info()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "invalid argument, missing txn info";
        return;
    }

    auto& txn_info = const_cast<TxnInfoPB&>(request->txn_info());
    std::string label = txn_info.has_label() ? txn_info.label() : "";
    int64_t db_id = txn_info.has_db_id() ? txn_info.db_id() : -1;

    if (label.empty() || db_id < 0 || txn_info.table_ids().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "invalid argument, label=" << label << " db_id=" << db_id;
        msg = ss.str();
        return;
    }

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    std::string instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "cannot find instance_id with cloud_unique_id="
           << (cloud_unique_id.empty() ? "(empty)" : cloud_unique_id) << " label=" << label;
        msg = ss.str();
        return;
    }

    //1. Generate version stamp for txn id
    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        ss << "txn_kv_->create_txn() failed, ret=" << ret << " label=" << label
           << " db_id=" << db_id;
        msg = ss.str();
        return;
    }

    std::string txn_label_key_;
    std::string txn_label_val;

    TxnLabelKeyInfo txn_label_key_info {instance_id, db_id, label};
    txn_label_key(txn_label_key_info, &txn_label_key_);

    ret = txn->get(txn_label_key_, &txn_label_val);
    if (ret < 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        ss << "txn->get failed(), ret=" << ret << " label=" << label;
        msg = ss.str();
        return;
    }

    LOG(INFO) << "txn->get txn_label_key=" << hex(txn_label_key_) << " label=" << label
              << " ret=" << ret;

    //ret == 0 means label has previous txn ids.
    if (ret == 0) {
        txn_label_val = txn_label_val.substr(0, txn_label_val.size() - VERSION_STAMP_LEN);
    }

    //ret > 0, means label not exist previously.
    txn->atomic_set_ver_value(txn_label_key_, txn_label_val);
    LOG(INFO) << "txn->atomic_set_ver_value txn_label_key=" << hex(txn_label_key_);

    TEST_SYNC_POINT_CALLBACK("begin_txn:before:commit_txn:1", &label);
    ret = txn->commit();
    TEST_SYNC_POINT_CALLBACK("begin_txn:after:commit_txn:1", &label);
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        std::stringstream ss;
        ss << "txn->commit failed(), label=" << label << " ret=" << ret;
        msg = ss.str();
        return;
    }
    //2. Get txn id from version stamp
    txn.reset();

    txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        ss << "failed to create txn when get txn id, label=" << label << " ret=" << ret;
        msg = ss.str();
        return;
    }

    txn_label_val.clear();
    ret = txn->get(txn_label_key_, &txn_label_val);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        ss << "txn->get() failed, label=" << label << " ret=" << ret;
        msg = ss.str();
        return;
    }

    LOG(INFO) << "txn->get txn_label_key=" << hex(txn_label_key_) << " label=" << label
              << " ret=" << ret;

    std::string txn_id_str =
            txn_label_val.substr(txn_label_val.size() - VERSION_STAMP_LEN, txn_label_val.size());
    // Generated by TxnKv system
    int64_t txn_id = 0;
    ret = get_txn_id_from_fdb_ts(txn_id_str, &txn_id);
    if (ret != 0) {
        code = MetaServiceCode::TXN_GEN_ID_ERR;
        ss << "get_txn_id_from_fdb_ts() failed, label=" << label << " ret=" << ret;
        msg = ss.str();
        return;
    }

    LOG(INFO) << "get_txn_id_from_fdb_ts() label=" << label << " txn_id=" << txn_id
              << " txn_label_val.size()=" << txn_label_val.size();

    TxnLabelPB txn_label_pb;
    if (txn_label_val.size() > VERSION_STAMP_LEN) {
        //3. Check label
        //txn_label_val.size() > VERSION_STAMP_LEN means label has previous txn ids.

        std::string txn_label_pb_str =
                txn_label_val.substr(0, txn_label_val.size() - VERSION_STAMP_LEN);
        if (!txn_label_pb.ParseFromString(txn_label_pb_str)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            ss << "txn_label_pb->ParseFromString() failed, txn_id=" << txn_id << " label=" << label;
            msg = ss.str();
            return;
        }

        // Check if label already used, by following steps
        // 1. get all existing transactions
        // 2. if there is a PREPARE transaction, check if this is a retry request.
        // 3. if there is a non-aborted transaction, throw label already used exception.

        for (auto& cur_txn_id : txn_label_pb.txn_ids()) {
            std::string cur_txn_inf_key;
            std::string cur_txn_inf_val;
            TxnInfoKeyInfo cur_txn_inf_key_info {instance_id, db_id, cur_txn_id};
            txn_info_key(cur_txn_inf_key_info, &cur_txn_inf_key);
            ret = txn->get(cur_txn_inf_key, &cur_txn_inf_val);
            if (ret < 0) {
                code = MetaServiceCode::KV_TXN_GET_ERR;
                ss << "txn->get() failed, cur_txn_id=" << cur_txn_id << " label=" << label
                   << " ret=" << ret;
                msg = ss.str();
                return;
            }

            if (ret == 1) {
                //label_to_idx and txn info inconsistency.
                code = MetaServiceCode::TXN_ID_NOT_FOUND;
                ss << "txn->get() failed, cur_txn_id=" << cur_txn_id << " label=" << label
                   << " ret=" << ret;
                msg = ss.str();
                return;
            }

            TxnInfoPB cur_txn_info;
            if (!cur_txn_info.ParseFromString(cur_txn_inf_val)) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                ss << "cur_txn_info->ParseFromString() failed, cur_txn_id=" << cur_txn_id
                   << " label=" << label << " ret=" << ret;
                msg = ss.str();
                return;
            }

            VLOG_DEBUG << "cur_txn_info:\n" << cur_txn_info.DebugString();
            if (cur_txn_info.status() == TxnStatusPB::TXN_STATUS_ABORTED) {
                continue;
            }

            if (cur_txn_info.status() == TxnStatusPB::TXN_STATUS_PREPARED ||
                cur_txn_info.status() == TxnStatusPB::TXN_STATUS_PRECOMMITTED) {
                // clang-format off
                if (cur_txn_info.has_request_id() && txn_info.has_request_id() &&
                    ((cur_txn_info.request_id().hi() == txn_info.request_id().hi()) && 
                     (cur_txn_info.request_id().lo() == txn_info.request_id().lo()))) {

                    response->set_dup_txn_id(cur_txn_info.txn_id());
                    code = MetaServiceCode::TXN_DUPLICATED_REQ;
                    ss << "db_id=" << db_id << " label=" << label << " txn_id=" << cur_txn_info.txn_id() << " dup begin txn request.";
                    msg = ss.str();
                    return;
                }
                // clang-format on
            }
            code = MetaServiceCode::TXN_LABEL_ALREADY_USED;
            ss << "db_id=" << db_id << " label=" << label
               << " already used by txn_id=" << cur_txn_info.txn_id();
            msg = ss.str();
            return;
        }
    }

    // Update txn_info to be put into TxnKv
    // Update txn_id in PB
    txn_info.set_txn_id(txn_id);
    // TODO:
    // check initial status must be TXN_STATUS_PREPARED or TXN_STATUS_UNKNOWN
    txn_info.set_status(TxnStatusPB::TXN_STATUS_PREPARED);

    auto now_time = system_clock::now();
    uint64_t prepare_time = duration_cast<milliseconds>(now_time.time_since_epoch()).count();

    txn_info.set_prepare_time(prepare_time);

    //4. put txn info and db_tbl
    std::string txn_inf_key;
    std::string txn_inf_val;
    TxnInfoKeyInfo txn_inf_key_info {instance_id, db_id, txn_id};
    txn_info_key(txn_inf_key_info, &txn_inf_key);
    if (!txn_info.SerializeToString(&txn_inf_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize txn_info, label=" << label << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    std::string txn_index_key_;
    std::string txn_index_val;
    TxnIndexKeyInfo txn_index_key_info {instance_id, txn_id};
    txn_index_key(txn_index_key_info, &txn_index_key_);
    TxnIndexPB txn_index_pb;
    txn_index_pb.mutable_tablet_index()->set_db_id(db_id);
    if (!txn_index_pb.SerializeToString(&txn_index_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize txn_index_pb "
           << "label=" << label << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    std::string txn_run_key;
    std::string txn_run_val;
    TxnRunningKeyInfo txn_run_key_info {instance_id, db_id, txn_id};
    txn_running_key(txn_run_key_info, &txn_run_key);

    TxnRunningPB running_val_pb;
    uint64_t txn_timeout_ms = txn_info.has_timeout_ms()
                                      ? txn_info.timeout_ms()
                                      : config::stream_load_default_timeout_second * 1000;

    running_val_pb.set_timeout_time(prepare_time + txn_timeout_ms);
    for (auto i : txn_info.table_ids()) {
        running_val_pb.add_table_ids(i);
    }
    VLOG_DEBUG << "label=" << label << " txn_id=" << txn_id << "running_val_pb:\n"
               << running_val_pb.DebugString();
    if (!running_val_pb.SerializeToString(&txn_run_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize running_val_pb label=" << label << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    txn_label_pb.add_txn_ids(txn_id);
    VLOG_DEBUG << "label=" << label << " txn_id=" << txn_id << "txn_label_pb:\n"
               << txn_label_pb.DebugString();
    if (!txn_label_pb.SerializeToString(&txn_label_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize txn_label_pb label=" << label << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }
    txn->atomic_set_ver_value(txn_label_key_, txn_label_val);
    LOG(INFO) << "txn->atomic_set_ver_value txn_label_key=" << hex(txn_label_key_)
              << " label=" << label << " txn_id=" << txn_id;

    txn->put(txn_inf_key, txn_inf_val);
    txn->put(txn_index_key_, txn_index_val);
    txn->put(txn_run_key, txn_run_val);
    LOG(INFO) << "xxx put txn_info_key=" << hex(txn_inf_key) << " txn_id=" << txn_id;
    LOG(INFO) << "xxx put txn_run_key=" << hex(txn_run_key) << " txn_id=" << txn_id;
    LOG(INFO) << "xxx put txn_index_key=" << hex(txn_index_key_) << " txn_id=" << txn_id;

    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        ss << "failed to commit txn kv, label=" << label << " txn_id=" << txn_id << " ret=" << ret;
        msg = ss.str();
        return;
    }
    TEST_SYNC_POINT_CALLBACK("begin_txn:after:commit_txn:2", &txn_id);
    response->set_txn_id(txn_id);
    return;
}

void MetaServiceImpl::precommit_txn(::google::protobuf::RpcController* controller,
                                    const ::selectdb::PrecommitTxnRequest* request,
                                    ::selectdb::PrecommitTxnResponse* response,
                                    ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request:\n" << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    [[maybe_unused]] std::stringstream ss;
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&code, &msg, &response, &ctrl](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << __PRETTY_FUNCTION__ << " finish " << ctrl->remote_side()
                          << " response:\n"
                          << response->DebugString();
            });

    int64_t txn_id = request->has_txn_id() ? request->txn_id() : -1;
    int64_t db_id = request->has_db_id() ? request->db_id() : -1;
    if (txn_id < 0 && db_id < 0) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "invalid txn_id and db_id, "
           << "txn_id=" << txn_id << " db_id=" << db_id;
        msg = ss.str();
        return;
    }

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    std::string instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "cannot find instance_id with cloud_unique_id="
           << (cloud_unique_id.empty() ? "(empty)" : cloud_unique_id) << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        ss << "txn_kv_->create_txn() failed, ret=" << ret << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    //not provide db_id, we need read from disk.
    if (db_id < 0) {
        std::string txn_index_key_;
        std::string txn_index_val;
        TxnIndexKeyInfo txn_index_key_info {instance_id, txn_id};
        txn_index_key(txn_index_key_info, &txn_index_key_);
        ret = txn->get(txn_index_key_, &txn_index_val);
        if (ret != 0) {
            code = ret > 0 ? MetaServiceCode::TXN_ID_NOT_FOUND : MetaServiceCode::KV_TXN_GET_ERR;
            ss << "failed to get db id with txn_id=" << txn_id << " ret=" << ret;
            msg = ss.str();
            return;
        }
        TxnIndexPB txn_index_pb;
        if (!txn_index_pb.ParseFromString(txn_index_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            ss << "failed to parse txn_inf"
               << " txn_id=" << txn_id;
            msg = ss.str();
            return;
        }
        DCHECK(txn_index_pb.has_tablet_index() == true);
        DCHECK(txn_index_pb.tablet_index().has_db_id() == true);
        db_id = txn_index_pb.tablet_index().db_id();
        VLOG_DEBUG << " find db_id=" << db_id << " from index";
    } else {
        db_id = request->db_id();
    }

    // Get txn info with db_id and txn_id
    std::string txn_inf_key; // Will be used when saving updated txn
    std::string txn_inf_val; // Will be reused when saving updated txn
    TxnInfoKeyInfo txn_inf_key_info {instance_id, db_id, txn_id};
    txn_info_key(txn_inf_key_info, &txn_inf_key);
    ret = txn->get(txn_inf_key, &txn_inf_val);
    if (ret != 0) {
        code = ret > 0 ? MetaServiceCode::TXN_ID_NOT_FOUND : MetaServiceCode::KV_TXN_GET_ERR;
        ss << "failed to get db id with db_id=" << db_id << " txn_id=" << txn_id << " ret=" << ret;
        msg = ss.str();
        return;
    }

    TxnInfoPB txn_info;
    if (!txn_info.ParseFromString(txn_inf_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        ss << "failed to parse txn_inf db_id=" << db_id << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    DCHECK(txn_info.txn_id() == txn_id);
    if (txn_info.status() == TxnStatusPB::TXN_STATUS_ABORTED) {
        code = MetaServiceCode::TXN_ALREADY_ABORTED;
        ss << "transaction is already aborted: db_id=" << db_id << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    if (txn_info.status() == TxnStatusPB::TXN_STATUS_VISIBLE) {
        code = MetaServiceCode::TXN_ALREADY_VISIBLE;
        ss << "ransaction is already visible: db_id=" << db_id << " txn_id=" << txn_id;
        msg = ss.str();
    }

    if (txn_info.status() == TxnStatusPB::TXN_STATUS_PRECOMMITTED) {
        code = MetaServiceCode::TXN_ALREADY_PRECOMMITED;
        ss << "ransaction is already precommited: db_id=" << db_id << " txn_id=" << txn_id;
        msg = ss.str();
    }

    LOG(INFO) << "before update txn_info:\n" << txn_info.DebugString();

    // Update txn_info
    txn_info.set_status(TxnStatusPB::TXN_STATUS_PRECOMMITTED);

    auto now_time = system_clock::now();
    uint64_t precommit_time = duration_cast<milliseconds>(now_time.time_since_epoch()).count();
    txn_info.set_precommit_time(precommit_time);
    if (request->has_commit_attachment()) {
        txn_info.mutable_commit_attachment()->CopyFrom(request->commit_attachment());
    }
    LOG(INFO) << "after update txn_info:\n" << txn_info.DebugString();

    txn_inf_val.clear();
    if (!txn_info.SerializeToString(&txn_inf_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize txn_info when saving, txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    LOG(INFO) << "xxx put txn_inf_key=" << hex(txn_inf_key) << " txn_id=" << txn_id;
    txn->put(txn_inf_key, txn_inf_val);

    std::string txn_run_key;
    std::string txn_run_val;
    TxnRunningKeyInfo txn_run_key_info {instance_id, db_id, txn_id};
    txn_running_key(txn_run_key_info, &txn_run_key);

    TxnRunningPB running_val_pb;
    uint64_t txn_timeout_ms = request->has_precommit_timeout_ms()
                                      ? txn_info.precommit_timeout_ms()
                                      : config::stream_load_default_precommit_timeout_second * 1000;
    running_val_pb.set_timeout_time(precommit_time + txn_timeout_ms);
    if (!running_val_pb.SerializeToString(&txn_run_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize running_val_pb, txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    LOG(INFO) << "xxx put txn_run_key=" << hex(txn_run_key) << " txn_id=" << txn_id;
    txn->put(txn_run_key, txn_run_val);

    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        ss << "failed to commit txn kv, txn_id=" << txn_id;
        msg = ss.str();
        return;
    }
}

/**
 * 0. Extract txn_id from request
 * 1. Get db id from TxnKv with txn_id
 * 2. Get TxnInfo from TxnKv with db_id and txn_id
 * 3. Get tmp rowset meta, there may be several or hundred of tmp rowsets
 * 4. Get versions of each rowset
 * 5. Put rowset meta, which will be visible to user
 * 6. Put TxnInfo back into TxnKv with updated txn status (committed)
 * 7. Update versions of each partition
 * 8. Remove tmp rowset meta
 *
 * Note: getting version and all changes maded are in a single TxnKv transaction:
 *       step 5, 6, 7, 8
 */
void MetaServiceImpl::commit_txn(::google::protobuf::RpcController* controller,
                                 const ::selectdb::CommitTxnRequest* request,
                                 ::selectdb::CommitTxnResponse* response,
                                 ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << __PRETTY_FUNCTION__ << " rpc from " << ctrl->remote_side() << " request:\n"
              << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    [[maybe_unused]] std::stringstream ss;

    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&code, &msg, &response, &ctrl](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                if (code != MetaServiceCode::OK) {
                    response->clear_table_ids();
                    response->clear_partition_ids();
                    response->clear_versions();
                }
                LOG(INFO) << __PRETTY_FUNCTION__ << " finish " << ctrl->remote_side()
                          << " response:\n"
                          << response->DebugString();
            });

    if (!request->has_txn_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "invalid argument, missing txn id";
        return;
    }

    int64_t txn_id = request->txn_id();

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    std::string instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id << " txn_id=" << txn_id;
        return;
    }

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        ss << "filed to create txn, txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    //Get db id with txn id
    std::string txn_index_key_;
    std::string txn_index_val;
    TxnIndexKeyInfo txn_index_key_info {instance_id, txn_id};
    txn_index_key(txn_index_key_info, &txn_index_key_);
    ret = txn->get(txn_index_key_, &txn_index_val);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        ss << "failed to get db id, txn_id=" << txn_id << " ret=" << ret;
        msg = ss.str();
        return;
    }

    TxnIndexPB txn_index_pb;
    if (!txn_index_pb.ParseFromString(txn_index_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        ss << "failed to parse txn_index_pb, txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    DCHECK(txn_index_pb.has_tablet_index() == true);
    DCHECK(txn_index_pb.tablet_index().has_db_id() == true);
    int64_t db_id = txn_index_pb.tablet_index().db_id();

    // Get txn info with db_id and txn_id
    std::string txn_inf_key; // Will be used when saving updated txn
    std::string txn_inf_val; // Will be reused when saving updated txn
    TxnInfoKeyInfo txn_inf_key_info {instance_id, db_id, txn_id};
    txn_info_key(txn_inf_key_info, &txn_inf_key);
    ret = txn->get(txn_inf_key, &txn_inf_val);
    if (ret != 0) {
        code = ret > 0 ? MetaServiceCode::TXN_ID_NOT_FOUND : MetaServiceCode::KV_TXN_GET_ERR;
        ss << "failed to get txn_info, db_id=" << db_id << " txn_id=" << txn_id << " ret=" << ret;
        msg = ss.str();
        return;
    }

    TxnInfoPB txn_info;
    if (!txn_info.ParseFromString(txn_inf_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        ss << "failed to parse txn_info, db_id=" << db_id << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    // TODO: do more check like txn state, 2PC etc.
    DCHECK(txn_info.txn_id() == txn_id);
    if (txn_info.status() == TxnStatusPB::TXN_STATUS_ABORTED) {
        code = MetaServiceCode::TXN_ALREADY_ABORTED;
        ss << "transaction is already aborted: db_id=" << db_id << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    if (txn_info.status() == TxnStatusPB::TXN_STATUS_VISIBLE) {
        code = MetaServiceCode::TXN_ALREADY_VISIBLE;
        ss << "transaction is already visible: db_id=" << db_id << " txn_id=" << txn_id;
        msg = ss.str();
        response->mutable_txn_info()->CopyFrom(txn_info);
        return;
    }

    if (request->has_is_2pc() && request->is_2pc() && TxnStatusPB::TXN_STATUS_PREPARED) {
        code = MetaServiceCode::TXN_INVALID_STATUS;
        ss << "transaction is prepare, not pre-committed: db_id=" << db_id << " txn_id" << txn_id;
        msg = ss.str();
        return;
    }

    LOG(INFO) << "txn_id=" << txn_id << " txn_info:\n" << txn_info.DebugString();

    // Get temporary rowsets involved in the txn
    // This is a range scan
    MetaRowsetTmpKeyInfo rs_tmp_key_info0 {instance_id, txn_id, 0};
    MetaRowsetTmpKeyInfo rs_tmp_key_info1 {instance_id, txn_id + 1, 0};
    std::string rs_tmp_key0;
    std::string rs_tmp_key1;
    meta_rowset_tmp_key(rs_tmp_key_info0, &rs_tmp_key0);
    meta_rowset_tmp_key(rs_tmp_key_info1, &rs_tmp_key1);
    // Get rowset meta that should be commited
    //                   tmp_rowset_key -> rowset_meta
    std::vector<std::pair<std::string, doris::RowsetMetaPB>> tmp_rowsets_meta;

    int num_rowsets = 0;
    std::unique_ptr<int, std::function<void(int*)>> defer_log_range(
            (int*)0x01, [rs_tmp_key0, rs_tmp_key1, &num_rowsets, &txn_id](int*) {
                LOG(INFO) << "get tmp rowset meta, txn_id=" << txn_id
                          << " num_rowsets=" << num_rowsets << " range=[" << hex(rs_tmp_key0) << ","
                          << hex(rs_tmp_key1) << ")";
            });

    std::unique_ptr<RangeGetIterator> it;
    do {
        ret = txn->get(rs_tmp_key0, rs_tmp_key1, &it);
        if (ret != 0) {
            code = MetaServiceCode::KV_TXN_GET_ERR;
            ss << "internal error, failed to get tmp rowset while committing, txn_id=" << txn_id
               << " ret=" << ret;
            msg = ss.str();
            return;
        }

        while (it->has_next()) {
            auto [k, v] = it->next();
            LOG(INFO) << "range_get rowset_tmp_key=" << hex(k) << " txn_id=" << txn_id;
            tmp_rowsets_meta.emplace_back();
            if (!tmp_rowsets_meta.back().second.ParseFromArray(v.data(), v.size())) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                ss << "malformed rowset meta, unable to initialize, txn_id=" << txn_id;
                msg = ss.str();
                ss << " key=" << hex(k);
                LOG(WARNING) << ss.str();
                return;
            }
            // Save keys that will be removed later
            tmp_rowsets_meta.back().first = std::string(k.data(), k.size());
            ++num_rowsets;
            if (!it->has_next()) rs_tmp_key0 = k;
        }
        rs_tmp_key0.push_back('\x00'); // Update to next smallest key for iteration
    } while (it->more());

    VLOG_DEBUG << "txn_id=" << txn_id << " tmp_rowsets_meta.size()=" << tmp_rowsets_meta.size();

    // Prepare rowset meta and new_versions
    std::vector<std::pair<std::string, std::string>> rowsets;
    std::map<std::string, uint64_t> new_versions;
    std::map<std::string, TabletStatsPB> tablet_stats;
    std::map<int64_t, TabletIndexPB> table_ids; // tablet_id -> {table/index/partition}_id
    rowsets.reserve(tmp_rowsets_meta.size());
    for (auto& [_, i] : tmp_rowsets_meta) {
        int64_t tablet_id = i.tablet_id();
        // Get version for the rowset
        if (table_ids.count(tablet_id) == 0) {
            MetaTabletIdxKeyInfo key_info {instance_id, tablet_id};
            auto [key, val] = std::make_tuple(std::string(""), std::string(""));
            meta_tablet_idx_key(key_info, &key);
            ret = txn->get(key, &val);
            if (ret != 0) { // Must be 0, an existing value
                code = MetaServiceCode::KV_TXN_GET_ERR;
                ss << "failed to get tablet table index ids,"
                   << (ret == 1 ? " not found" : " internal error") << " tablet_id=" << tablet_id
                   << " key=" << hex(key);
                msg = ss.str();
                LOG(INFO) << msg << " ret=" << ret << " txn_id=" << txn_id;
                return;
            }
            if (!table_ids[tablet_id].ParseFromString(val)) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                ss << "malformed tablet index value tablet_id=" << tablet_id
                   << " txn_id=" << txn_id;
                msg = ss.str();
                return;
            }
            VLOG_DEBUG << "tablet_id:" << tablet_id
                       << " value:" << table_ids[tablet_id].DebugString();
        }

        int64_t table_id = table_ids[tablet_id].table_id();
        int64_t index_id = table_ids[tablet_id].index_id();
        int64_t partition_id = i.partition_id();

        VersionKeyInfo ver_key_info {instance_id, db_id, table_id, partition_id};
        std::string ver_key;
        version_key(ver_key_info, &ver_key);
        int64_t version = -1;
        std::string ver_val_str;
        int64_t new_version = -1;
        VersionPB version_pb;
        if (new_versions.count(ver_key) == 0) {
            ret = txn->get(ver_key, &ver_val_str);
            if (ret != 1 && ret != 0) {
                code = MetaServiceCode::KV_TXN_GET_ERR;
                ss << "failed to get version, table_id=" << table_id
                   << "partition_id=" << partition_id << " key=" << hex(ver_key);
                msg = ss.str();
                LOG(INFO) << msg << " txn_id=" << txn_id;
                return;
            }

            if (ret == 1) {
                // Maybe first version
                version = 1;
            } else {
                if (!version_pb.ParseFromString(ver_val_str)) {
                    code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                    ss << "failed to parse ver_val_str"
                       << " txn_id=" << txn_id << " key=" << hex(ver_key);
                    msg = ss.str();
                    return;
                }
                version = version_pb.version();
            }
            new_version = version + 1;
            new_versions.insert({std::move(ver_key), new_version});
        } else {
            new_version = new_versions[ver_key];
        }

        // Update rowset version
        i.set_start_version(new_version);
        i.set_end_version(new_version);

        std::string key, val;
        MetaRowsetKeyInfo key_info {instance_id, tablet_id, i.end_version()};
        meta_rowset_key(key_info, &key);
        if (!i.SerializeToString(&val)) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            ss << "failed to serialize rowset_meta, txn_id=" << txn_id;
            msg = ss.str();
            return;
        }
        rowsets.emplace_back(std::move(key), std::move(val));

        // Accumulate affected rows
        StatsTabletKeyInfo stat_key_info {instance_id, table_id, index_id, partition_id, tablet_id};
        std::string stat_key, stat_val;
        stats_tablet_key(stat_key_info, &stat_key);
        while (tablet_stats.count(stat_key) == 0) {
            ret = txn->get(stat_key, &stat_val); // May be perf. bottle-neck
            LOG(INFO) << "get tablet_stats_key, key=" << hex(stat_key) << " ret=" << ret
                      << " txn_id=" << txn_id;
            if (ret != 1 && ret != 0) {
                code = MetaServiceCode::KV_TXN_GET_ERR;
                ss << "failed to get tablet stats, tablet_id=" << tablet_id
                   << " key=" << hex(stat_key);
                msg = ss.str();
                LOG(INFO) << msg << " txn_id=" << txn_id;
                return;
            }
            auto& stat = tablet_stats[stat_key];
            if (ret == 1) { // First time stats, initialize it
                stat.mutable_idx()->set_table_id(table_id);
                stat.mutable_idx()->set_index_id(index_id);
                stat.mutable_idx()->set_partition_id(partition_id);
                stat.mutable_idx()->set_tablet_id(tablet_id);
                break;
            }
            if (!stat.ParseFromString(stat_val)) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = "malformed tablet table value";
                return;
            }
            CHECK_EQ(stat.mutable_idx()->table_id(), table_id);
            CHECK_EQ(stat.mutable_idx()->index_id(), index_id);
            CHECK_EQ(stat.mutable_idx()->partition_id(), partition_id);
            CHECK_EQ(stat.mutable_idx()->tablet_id(), tablet_id);
            if (stat.num_rowsets() > config::max_tablet_version_num) {
                code = MetaServiceCode::UNDEFINED_ERR;
                ss << "tablet exceeds max version num limit, txn_id=" << txn_id
                   << " tablet_id=" << tablet_id << " limit=" << config::max_tablet_version_num;
                msg = ss.str();
                return;
            }
            break;
        }
        auto& stat = tablet_stats[stat_key];
        stat.set_data_size(stat.data_size() + i.data_disk_size());
        stat.set_num_rows(stat.num_rows() + i.num_rows());
        stat.set_num_rowsets(stat.num_rowsets() + 1);
        stat.set_num_segments(stat.num_segments() + i.num_segments());
    } // for tmp_rowsets_meta

    // Save rowset meta
    for (auto& i : rowsets) {
        txn->put(i.first, i.second);
        LOG(INFO) << "xxx put rowset_key=" << hex(i.first) << " txn_id=" << txn_id;
    }

    // Save versions
    for (auto& i : new_versions) {
        std::string ver_val;
        VersionPB version_pb;
        version_pb.set_version(i.second);
        if (!version_pb.SerializeToString(&ver_val)) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            ss << "failed to serialize version_pb when saving, txn_id=" << txn_id;
            msg = ss.str();
            return;
        }

        txn->put(i.first, ver_val);
        LOG(INFO) << "xxx put version_key=" << hex(i.first) << " version:" << i.second
                  << " txn_id=" << txn_id;

        std::string_view ver_key = i.first;
        //VersionKeyInfo  {instance_id, db_id, table_id, partition_id}
        ver_key.remove_prefix(1); // Remove key space
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        ret = decode_key(&ver_key, &out);
        if (ret != 0) [[unlikely]] {
            // decode version key error means this is something wrong,
            // we can not continue this txn
            LOG(WARNING) << "failed to decode key, ret=" << ret << " key=" << hex(ver_key);
            code = MetaServiceCode::UNDEFINED_ERR;
            msg = "decode version key error";
            return;
        }

        int64_t table_id = std::get<int64_t>(std::get<0>(out[4]));
        int64_t partition_id = std::get<int64_t>(std::get<0>(out[5]));
        VLOG_DEBUG << " table_id=" << table_id << " partition_id=" << partition_id;

        response->add_table_ids(table_id);
        response->add_partition_ids(partition_id);
        response->add_versions(i.second);
    }

    LOG(INFO) << " before update txn_info:\n" << txn_info.DebugString();

    // Update txn_info
    txn_info.set_status(TxnStatusPB::TXN_STATUS_VISIBLE);

    auto now_time = system_clock::now();
    uint64_t commit_time = duration_cast<milliseconds>(now_time.time_since_epoch()).count();
    txn_info.set_commit_time(commit_time);
    txn_info.set_finish_time(commit_time);
    if (request->has_commit_attachment()) {
        txn_info.mutable_commit_attachment()->CopyFrom(request->commit_attachment());
    }
    LOG(INFO) << "after update txn_info:\n" << txn_info.DebugString();
    txn_inf_val.clear();
    if (!txn_info.SerializeToString(&txn_inf_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize txn_info when saving, txn_id=" << txn_id;
        msg = ss.str();
        return;
    }
    txn->put(txn_inf_key, txn_inf_val);
    LOG(INFO) << "xxx put txn_inf_key=" << hex(txn_inf_key) << " txn_id=" << txn_id;

    // Update stats of affected tablet
    for (auto& [k, v] : tablet_stats) {
        std::string val = v.SerializeAsString();
        if (val.empty()) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            ss << "failed to serialize tablet stats when saving, key=" << hex(k)
               << " txn_id=" << txn_id;
            return;
        }
        txn->put(k, val);
        LOG(INFO) << "xxx put tablet_stat_key key=" << hex(k) << " txn_id=" << txn_id;
        VLOG_DEBUG << "xxx put tablet_stat_key key=" << hex(k) << " txn_id=" << txn_id
                   << " stats=" << proto_to_json(v);
    }

    // Remove tmp rowset meta
    for (auto& [k, _] : tmp_rowsets_meta) {
        txn->remove(k);
        LOG(INFO) << "xxx remove tmp_rowset_key=" << hex(k) << " txn_id=" << txn_id;
    }

    std::string txn_run_key;
    TxnRunningKeyInfo txn_run_key_info {instance_id, db_id, txn_id};
    txn_running_key(txn_run_key_info, &txn_run_key);
    LOG(INFO) << "xxx remove txn_run_key=" << hex(txn_run_key) << " txn_id=" << txn_id;
    txn->remove(txn_run_key);

    std::string recycle_txn_key_;
    std::string recycle_txn_val;
    RecycleTxnKeyInfo recycle_txn_key_info {instance_id, db_id, txn_id};

    recycle_txn_key(recycle_txn_key_info, &recycle_txn_key_);
    RecycleTxnPB recycle_txn_pb;
    recycle_txn_pb.set_creation_time(commit_time);
    recycle_txn_pb.set_label(txn_info.label());

    if (!recycle_txn_pb.SerializeToString(&recycle_txn_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize running_val_pb, txn_id=" << txn_id;
        msg = ss.str();
        return;
    }
    txn->put(recycle_txn_key_, recycle_txn_val);
    LOG(INFO) << "xxx put recycle_txn_key key=" << hex(recycle_txn_key_) << " txn_id=" << txn_id;

    // Finally we are done...
    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        ss << "failed to commit kv txn, txn_id=" << txn_id << " ret=" << ret;
        msg = ss.str();
        return;
    }
    response->mutable_txn_info()->CopyFrom(txn_info);
}

void MetaServiceImpl::abort_txn(::google::protobuf::RpcController* controller,
                                const ::selectdb::AbortTxnRequest* request,
                                ::selectdb::AbortTxnResponse* response,
                                ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << __PRETTY_FUNCTION__ << " rpc from " << ctrl->remote_side() << " request:\n"
              << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    [[maybe_unused]] std::stringstream ss;
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&code, &msg, &response, &ctrl](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << __PRETTY_FUNCTION__ << " finish " << ctrl->remote_side()
                          << " response:\n"
                          << response->DebugString();
            });

    // Get txn id
    int64_t txn_id = request->has_txn_id() ? request->txn_id() : -1;
    std::string label = request->has_label() ? request->label() : "";
    int64_t db_id = request->has_db_id() ? request->db_id() : -1;
    if (txn_id < 0 && (label.empty() || db_id < 0)) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "invalid txn id and label, db_id=" << db_id << " txn_id=" << txn_id
           << " label=" << label;
        msg = ss.str();
        return;
    }

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    std::string instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "cannot find instance_id with cloud_unique_id="
           << (cloud_unique_id.empty() ? "(empty)" : cloud_unique_id) << " label=" << label
           << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        ss << "filed to txn_kv_->create_txn(), txn_id=" << txn_id << " label=" << label;
        msg = ss.str();
        return;
    }

    std::string txn_inf_key; // Will be used when saving updated txn
    std::string txn_inf_val; // Will be reused when saving updated txn
    TxnInfoPB txn_info;

    //TODO: split with two function.
    //there two ways to abort txn:
    //1. abort txn by txn id
    //2. abort txn by label and db_id
    if (txn_id > 0) {
        VLOG_DEBUG << "abort_txn by txn_id";
        //abort txn by txn id
        // Get db id with txn id

        std::string txn_index_key_;
        std::string txn_index_val;

        //not provide db_id, we need read from disk.
        if (!request->has_db_id()) {
            TxnIndexKeyInfo txn_index_key_info {instance_id, txn_id};
            txn_index_key(txn_index_key_info, &txn_index_key_);
            ret = txn->get(txn_index_key_, &txn_index_val);
            if (ret != 0) {
                code = ret > 0 ? MetaServiceCode::TXN_ID_NOT_FOUND
                               : MetaServiceCode::KV_TXN_GET_ERR;
                ss << "failed to get db id, txn_id=" << txn_id << " ret=" << ret;
                msg = ss.str();
                return;
            }

            TxnIndexPB txn_index_pb;
            if (!txn_index_pb.ParseFromString(txn_index_val)) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                ss << "failed to parse txn_index_val"
                   << " txn_id=" << txn_id;
                msg = ss.str();
                return;
            }
            DCHECK(txn_index_pb.has_tablet_index() == true);
            DCHECK(txn_index_pb.tablet_index().has_db_id() == true);
            db_id = txn_index_pb.tablet_index().db_id();
        } else {
            db_id = request->db_id();
        }

        // Get txn info with db_id and txn_id
        TxnInfoKeyInfo txn_inf_key_info {instance_id, db_id, txn_id};
        txn_info_key(txn_inf_key_info, &txn_inf_key);
        ret = txn->get(txn_inf_key, &txn_inf_val);
        if (ret != 0) {
            code = ret > 0 ? MetaServiceCode::TXN_ID_NOT_FOUND : MetaServiceCode::KV_TXN_GET_ERR;
            ss << "failed to get txn_info, db_id=" << db_id << "txn_id=" << txn_id << "ret=" << ret;
            msg = ss.str();
            return;
        }

        if (!txn_info.ParseFromString(txn_inf_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            ss << "failed to parse txn_info db_id=" << db_id << "txn_id=" << txn_id;
            msg = ss.str();
            return;
        }

        DCHECK(txn_info.txn_id() == txn_id);

        //check state is valid.
        if (txn_info.status() == TxnStatusPB::TXN_STATUS_ABORTED) {
            code = MetaServiceCode::TXN_ALREADY_ABORTED;
            ss << "transaction is already abort db_id=" << db_id << "txn_id=" << txn_id;
            msg = ss.str();
            return;
        }
        if (txn_info.status() == TxnStatusPB::TXN_STATUS_VISIBLE) {
            code = MetaServiceCode::TXN_ALREADY_VISIBLE;
            ss << "transaction is already visible db_id=" << db_id << "txn_id=" << txn_id;
            msg = ss.str();
            return;
        }
    } else {
        VLOG_DEBUG << "abort_txn by db_id and txn label";
        //abort txn by label.
        std::string txn_label_key_;
        std::string txn_label_val;

        TxnLabelKeyInfo txn_label_key_info {instance_id, db_id, label};
        txn_label_key(txn_label_key_info, &txn_label_key_);
        ret = txn->get(txn_label_key_, &txn_label_val);
        if (ret < 0) {
            code = MetaServiceCode::KV_TXN_GET_ERR;
            ss << "txn->get() failed, label=" << label << " ret=" << ret;
            msg = ss.str();
            return;
        }
        //label index not exist
        if (ret > 0) {
            code = MetaServiceCode::TXN_LABEL_NOT_FOUND;
            ss << "label not found, db_id=" << db_id << " label=" << label << " ret=" << ret;
            msg = ss.str();
            return;
        }

        TxnLabelPB txn_label_pb;
        DCHECK(txn_label_val.size() > 10);
        std::string txn_label_pb_str = txn_label_val.substr(0, txn_label_val.size() - 10);
        if (!txn_label_pb.ParseFromString(txn_label_pb_str)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            ss << "txn_label_pb->ParseFromString() failed, label=" << label;
            msg = ss.str();
            return;
        }

        int64_t prepare_txn_id = 0;
        //found prepare state txn for abort
        for (auto& cur_txn_id : txn_label_pb.txn_ids()) {
            std::string cur_txn_inf_key;
            std::string cur_txn_inf_val;
            TxnInfoKeyInfo cur_txn_inf_key_info {instance_id, db_id, cur_txn_id};
            txn_info_key(cur_txn_inf_key_info, &cur_txn_inf_key);
            ret = txn->get(cur_txn_inf_key, &cur_txn_inf_val);
            if (ret != 0) {
                code = MetaServiceCode::KV_TXN_GET_ERR;
                std::stringstream ss;
                ss << "txn->get() failed, cur_txn_id=" << cur_txn_id << " ret=" << ret;
                msg = ss.str();
                return;
            }
            // ret == 0
            TxnInfoPB cur_txn_info;
            if (!cur_txn_info.ParseFromString(cur_txn_inf_val)) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                std::stringstream ss;
                ss << "cur_txn_info->ParseFromString() failed, cur_txn_id=" << cur_txn_id;
                msg = ss.str();
                return;
            }
            VLOG_DEBUG << "cur_txn_info:\n" << cur_txn_info.DebugString();
            //TODO: 2pc alse need to check TxnStatusPB::TXN_STATUS_PRECOMMITTED
            if ((cur_txn_info.status() == TxnStatusPB::TXN_STATUS_PREPARED) ||
                (cur_txn_info.status() == TxnStatusPB::TXN_STATUS_PRECOMMITTED)) {
                prepare_txn_id = cur_txn_id;
                txn_info = std::move(cur_txn_info);
                txn_inf_key = std::move(cur_txn_inf_key);
                break;
            }
        }

        if (prepare_txn_id == 0) {
            code = MetaServiceCode::TXN_INVALID_STATUS;
            std::stringstream ss;
            ss << "running transaction not found, db_id=" << db_id << " label=" << label;
            msg = ss.str();
            return;
        }
    }

    auto now_time = system_clock::now();
    uint64_t finish_time = duration_cast<milliseconds>(now_time.time_since_epoch()).count();

    // Update txn_info
    txn_info.set_status(TxnStatusPB::TXN_STATUS_ABORTED);
    txn_info.set_finish_time(finish_time);
    request->has_reason() ? txn_info.set_reason(request->reason())
                          : txn_info.set_reason("User Abort");

    if (request->has_commit_attachment()) {
        TxnCommitAttachmentPB attachement = request->commit_attachment();
        txn_info.mutable_commit_attachment()->CopyFrom(request->commit_attachment());
    }

    txn_inf_val.clear();
    if (!txn_info.SerializeToString(&txn_inf_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize txn_info when saving, txn_id=" << txn_info.txn_id();
        msg = ss.str();
        return;
    }
    LOG(INFO) << "check watermark conflict, txn_info:\n" << txn_info.DebugString();
    txn->put(txn_inf_key, txn_inf_val);
    LOG(INFO) << "xxx put txn_inf_key=" << hex(txn_inf_key) << " txn_id=" << txn_info.txn_id();

    std::string txn_run_key;
    TxnRunningKeyInfo txn_run_key_info {instance_id, db_id, txn_id};
    txn_running_key(txn_run_key_info, &txn_run_key);
    txn->remove(txn_run_key);
    LOG(INFO) << "xxx remove txn_run_key=" << hex(txn_run_key) << " txn_id=" << txn_info.txn_id();

    std::string recycle_txn_key_;
    std::string recycle_txn_val;
    RecycleTxnKeyInfo recycle_txn_key_info {instance_id, db_id, txn_id};
    recycle_txn_key(recycle_txn_key_info, &recycle_txn_key_);
    RecycleTxnPB recycle_txn_pb;
    recycle_txn_pb.set_creation_time(finish_time);
    recycle_txn_pb.set_label(txn_info.label());

    if (!recycle_txn_pb.SerializeToString(&recycle_txn_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize running_val_pb, txn_id=" << txn_info.txn_id();
        msg = ss.str();
        return;
    }
    txn->put(recycle_txn_key_, recycle_txn_val);
    LOG(INFO) << "xxx put recycle_txn_key=" << hex(recycle_txn_key_)
              << " txn_id=" << txn_info.txn_id();

    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        ss << "failed to commit kv txn, txn_id=" << txn_info.txn_id();
        msg = ss.str();
        return;
    }
    response->mutable_txn_info()->CopyFrom(txn_info);
}

void MetaServiceImpl::get_txn(::google::protobuf::RpcController* controller,
                              const ::selectdb::GetTxnRequest* request,
                              ::selectdb::GetTxnResponse* response,
                              ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << __PRETTY_FUNCTION__ << " rpc from " << ctrl->remote_side() << " request:\n"
              << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    [[maybe_unused]] std::stringstream ss;
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&code, &msg, &response, &ctrl](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << __PRETTY_FUNCTION__ << " finish " << ctrl->remote_side()
                          << " response:\n"
                          << response->DebugString();
            });

    int64_t txn_id = request->has_txn_id() ? request->txn_id() : -1;
    int64_t db_id = request->has_db_id() ? request->db_id() : -1;
    if (txn_id < 0) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "invalid txn_id, it may be not given or set properly, txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    std::string instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "cannot find instance_id with cloud_unique_id="
           << (cloud_unique_id.empty() ? "(empty)" : cloud_unique_id);
        msg = ss.str();
        return;
    }

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        ss << "failed to create txn, txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    //not provide db_id, we need read from disk.
    if (db_id < 0) {
        std::string txn_index_key_;
        std::string txn_index_val;
        TxnIndexKeyInfo txn_index_key_info {instance_id, txn_id};
        txn_index_key(txn_index_key_info, &txn_index_key_);
        ret = txn->get(txn_index_key_, &txn_index_val);
        if (ret != 0) {
            code = ret > 0 ? MetaServiceCode::TXN_ID_NOT_FOUND : MetaServiceCode::KV_TXN_GET_ERR;
            ss << "failed to get db id with txn_id=" << txn_id << " ret=" << ret;
            msg = ss.str();
            return;
        }

        TxnIndexPB txn_index_pb;
        if (!txn_index_pb.ParseFromString(txn_index_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            ss << "failed to parse txn_inf"
               << " txn_id=" << txn_id;
            msg = ss.str();
            return;
        }
        DCHECK(txn_index_pb.has_tablet_index() == true);
        DCHECK(txn_index_pb.tablet_index().has_db_id() == true);
        db_id = txn_index_pb.tablet_index().db_id();
        if (db_id <= 0) {
            ss << "internal error: unexpected db_id " << db_id;
            code = MetaServiceCode::UNDEFINED_ERR;
            msg = ss.str();
            return;
        }
    }

    // Get txn info with db_id and txn_id
    std::string txn_inf_key; // Will be used when saving updated txn
    std::string txn_inf_val; // Will be reused when saving updated txn
    TxnInfoKeyInfo txn_inf_key_info {instance_id, db_id, txn_id};
    txn_info_key(txn_inf_key_info, &txn_inf_key);
    ret = txn->get(txn_inf_key, &txn_inf_val);
    if (ret != 0) {
        code = ret > 0 ? MetaServiceCode::TXN_ID_NOT_FOUND : MetaServiceCode::KV_TXN_GET_ERR;
        ss << "failed to get db id with db_id=" << db_id << " txn_id=" << txn_id << " ret=" << ret;
        msg = ss.str();
        return;
    }

    TxnInfoPB txn_info;
    if (!txn_info.ParseFromString(txn_inf_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        ss << "failed to parse txn_inf db_id=" << db_id << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    VLOG_DEBUG << "txn_info:\n" << txn_info.DebugString();
    DCHECK(txn_info.txn_id() == txn_id);
    response->mutable_txn_info()->CopyFrom(txn_info);
    return;
}

//To get current max txn id for schema change watermark etc.
void MetaServiceImpl::get_current_max_txn_id(::google::protobuf::RpcController* controller,
                                             const ::selectdb::GetCurrentMaxTxnRequest* request,
                                             ::selectdb::GetCurrentMaxTxnResponse* response,
                                             ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << __PRETTY_FUNCTION__ << " rpc from " << ctrl->remote_side() << " request:\n"
              << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    std::unique_ptr<Transaction> txn;
    [[maybe_unused]] std::stringstream ss;
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&code, &msg, &response, &ctrl](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << __PRETTY_FUNCTION__ << " finish " << ctrl->remote_side()
                          << " response:\n"
                          << response->DebugString();
            });

    // TODO: For auth
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    std::string instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }

    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        msg = "failed to create txn";
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        return;
    }

    std::string key = "schema change";
    std::string val;
    ret = txn->get(key, &val);
    if (ret < 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        std::stringstream ss;
        ss << "txn->get() failed,"
           << " ret=" << ret;
        msg = ss.str();
        return;
    }
    int64_t read_version = txn->get_read_version();
    int64_t current_max_txn_id = read_version << 10;
    VLOG_DEBUG << "read_version=" << read_version << " current_max_txn_id=" << current_max_txn_id;
    response->set_current_max_txn_id(current_max_txn_id);
}

void MetaServiceImpl::check_txn_conflict(::google::protobuf::RpcController* controller,
                                         const ::selectdb::CheckTxnConflictRequest* request,
                                         ::selectdb::CheckTxnConflictResponse* response,
                                         ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << __PRETTY_FUNCTION__ << " rpc from " << ctrl->remote_side() << " request:\n"
              << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::unique_ptr<Transaction> txn;

    std::string msg = "OK";
    [[maybe_unused]] std::stringstream ss;

    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&code, &msg, &response, &ctrl](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << __PRETTY_FUNCTION__ << " finish " << ctrl->remote_side()
                          << " response:\n"
                          << response->DebugString();
            });

    if (!request->has_db_id() || !request->has_end_txn_id() || (request->table_ids_size() <= 0)) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "invalid db id, end txn id or table_ids.";
        LOG(WARNING) << msg << " request:\n" << request->DebugString();
        return;
    }
    // TODO: For auth
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    std::string instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "cannot find instance_id with cloud_unique_id="
           << (cloud_unique_id.empty() ? "(empty)" : cloud_unique_id);
        msg = ss.str();
        return;
    }

    int64_t db_id = request->db_id();
    std::string begin_txn_run_key;
    std::string begin_txn_run_val;
    std::string end_txn_run_key;
    std::string end_txn_run_val;
    TxnRunningKeyInfo begin_txn_run_key_info {instance_id, db_id, 0};
    TxnRunningKeyInfo end_txn_run_key_info {instance_id, db_id, request->end_txn_id()};
    txn_running_key(begin_txn_run_key_info, &begin_txn_run_key);
    txn_running_key(end_txn_run_key_info, &end_txn_run_key);
    LOG(INFO) << "begin_txn_run_key:" << hex(begin_txn_run_key)
              << " end_txn_run_key:" << hex(end_txn_run_key);

    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        msg = "failed to create txn";
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        return;
    }

    //TODO: use set to replace
    std::vector<int64_t> src_table_ids(request->table_ids().begin(), request->table_ids().end());
    std::sort(src_table_ids.begin(), src_table_ids.end());
    std::unique_ptr<RangeGetIterator> it;
    do {
        ret = txn->get(begin_txn_run_key, end_txn_run_key, &it, 1000);
        if (ret != 0) {
            code = MetaServiceCode::KV_TXN_GET_ERR;
            ss << "failed to get txn running info. ret=" << ret;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }

        VLOG_DEBUG << "begin_txn_run_key=" << hex(begin_txn_run_key)
                   << " end_txn_run_val=" << hex(end_txn_run_val)
                   << " it->has_next()=" << it->has_next();

        while (it->has_next()) {
            auto [k, v] = it->next();
            LOG(INFO) << "check watermark conflict range_get txn_run_key=" << hex(k);
            TxnRunningPB running_val_pb;
            if (!running_val_pb.ParseFromArray(v.data(), v.size())) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                ss << "malformed txn running info";
                msg = ss.str();
                ss << " key=" << hex(k);
                LOG(WARNING) << ss.str();
                return;
            }
            LOG(INFO) << "check watermark conflict range_get txn_run_key=" << hex(k)
                      << " running_val_pb=" << running_val_pb.DebugString();
            std::vector<int64_t> running_table_ids(running_val_pb.table_ids().begin(),
                                                   running_val_pb.table_ids().end());
            std::sort(running_table_ids.begin(), running_table_ids.end());
            std::vector<int64_t> result(std::min(running_table_ids.size(), src_table_ids.size()));
            std::vector<int64_t>::iterator iter = std::set_intersection(
                    src_table_ids.begin(), src_table_ids.end(), running_table_ids.begin(),
                    running_table_ids.end(), result.begin());
            result.resize(iter - result.begin());
            if (result.size() > 0) {
                response->set_finished(false);
                return;
            }

            if (!it->has_next()) {
                begin_txn_run_key = k;
            }
        }
        begin_txn_run_key.push_back('\x00'); // Update to next smallest key for iteration
    } while (it->more());
    response->set_finished(true);
}

void MetaServiceImpl::get_version(::google::protobuf::RpcController* controller,
                                  const ::selectdb::GetVersionRequest* request,
                                  ::selectdb::GetVersionResponse* response,
                                  ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request=" << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    std::unique_ptr<Transaction> txn;
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&code, &msg, &response, &ctrl](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << "rpc from " << ctrl->remote_side()
                          << " response=" << response->DebugString();
            });

    // TODO(dx): For auth
    std::string cloud_unique_id;
    if (request->has_cloud_unique_id()) {
        cloud_unique_id = request->cloud_unique_id();
    }

    int64_t db_id = request->has_db_id() ? request->db_id() : -1;
    int64_t table_id = request->has_table_id() ? request->table_id() : -1;
    int64_t partition_id = request->has_partition_id() ? request->partition_id() : -1;
    if (db_id == -1 || table_id == -1 || partition_id == -1) {
        msg = "params error, db_id=" + std::to_string(db_id) +
              " table_id=" + std::to_string(table_id) +
              " partition_id=" + std::to_string(partition_id);
        code = MetaServiceCode::INVALID_ARGUMENT;
        LOG(WARNING) << msg;
        return;
    }

    std::string instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }

    VersionKeyInfo ver_key_info {instance_id, db_id, table_id, partition_id};
    std::string ver_key;
    version_key(ver_key_info, &ver_key);

    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        msg = "failed to create txn";
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        return;
    }

    std::string ver_val;
    VersionPB version_pb;
    // 0 for success get a key, 1 for key not found, negative for error
    ret = txn->get(ver_key, &ver_val);
    LOG(INFO) << "xxx get version_key=" << hex(ver_key);
    if (ret == 0) {
        if (!version_pb.ParseFromString(ver_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = "malformed version value";
            return;
        }
        response->set_version(version_pb.version());
        return;
    } else if (ret == 1) {
        msg = "not found";
        code = MetaServiceCode::VERSION_NOT_FOUND;
        return;
    }
    msg = "failed to get txn";
    code = MetaServiceCode::KV_TXN_GET_ERR;
}

MetaServiceResponseStatus internal_create_tablet(doris::TabletMetaPB& tablet_meta,
        std::shared_ptr<TxnKv> txn_kv, const std::string& instance_id) {
    MetaServiceResponseStatus st;
    bool has_first_rowset = tablet_meta.rs_metas_size() > 0;
    doris::RowsetMetaPB first_rowset;
    if (has_first_rowset) {
        first_rowset.CopyFrom(tablet_meta.rs_metas(0));
        tablet_meta.clear_rs_metas(); // Strip off rowset meta
    }

    // TODO: validate tablet meta, check existence
    int64_t table_id = tablet_meta.table_id();
    int64_t index_id = tablet_meta.index_id();
    int64_t partition_id = tablet_meta.partition_id();
    int64_t tablet_id = tablet_meta.tablet_id();

    std::unique_ptr<Transaction> txn;
    int ret = txn_kv->create_txn(&txn);

    MetaTabletKeyInfo key_info {instance_id, table_id, index_id, partition_id, tablet_id};
    std::string key;
    std::string val;
    meta_tablet_key(key_info, &key);
    if (!tablet_meta.SerializeToString(&val)) {
        st.set_code(MetaServiceCode::PROTOBUF_SERIALIZE_ERR);
        st.set_msg("failed to serialize tablet meta");
        return st;
    }
    txn->put(key, val);
    LOG(INFO) << "xxx put tablet_key=" << hex(key);

    // Put first rowset if needed
    std::string rs_key;
    std::string rs_val;
    if (has_first_rowset) {
        MetaRowsetKeyInfo rs_key_info {instance_id, tablet_id, first_rowset.end_version()};
        meta_rowset_key(rs_key_info, &rs_key);
        if (!first_rowset.SerializeToString(&rs_val)) {
            st.set_code(MetaServiceCode::PROTOBUF_SERIALIZE_ERR);
            st.set_msg("failed to serialize first rowset meta");
            return st;
        }
        txn->put(rs_key, rs_val);
        LOG(INFO) << "xxx rowset key=" << hex(rs_key);
    }

    // Index tablet_id -> table_id, index_id, partition_id
    std::string key1;
    std::string val1;
    MetaTabletIdxKeyInfo key_info1 {instance_id, tablet_id};
    meta_tablet_idx_key(key_info1, &key1);
    TabletIndexPB tablet_table;
    // tablet_table.set_db_id(db_id);
    tablet_table.set_table_id(table_id);
    tablet_table.set_index_id(index_id);
    tablet_table.set_partition_id(partition_id);
    tablet_table.set_tablet_id(tablet_id);
    if (!tablet_table.SerializeToString(&val1)) {
        st.set_code(MetaServiceCode::PROTOBUF_SERIALIZE_ERR);
        st.set_msg("failed to serialize tablet table value");
        return st;
    }
    txn->put(key1, val1);
    LOG(INFO) << "put tablet_idx tablet_id=" << tablet_id << " key=" << hex(key1);

    // Create stats info for the tablet
    auto stats_key = stats_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    std::string stats_val;
    TabletStatsPB stats_pb;
    stats_pb.set_num_rowsets(1);
    stats_pb.set_num_segments(0);
    stats_pb.mutable_idx()->set_table_id(table_id);
    stats_pb.mutable_idx()->set_index_id(index_id);
    stats_pb.mutable_idx()->set_partition_id(partition_id);
    stats_pb.mutable_idx()->set_tablet_id(tablet_id);
    stats_pb.set_base_compaction_cnt(0);
    stats_pb.set_cumulative_compaction_cnt(0);
    // set cumulative point to 2 to not compact rowset [0-1]
    stats_pb.set_cumulative_point(2);
    stats_val = stats_pb.SerializeAsString();
    DCHECK(!stats_val.empty());
    txn->put(stats_key, stats_val);
    LOG(INFO) << "put tablet stats, tablet_id=" << tablet_id << " key=" << hex(stats_key);

    ret = txn->commit();
    if (ret != 0) {
        st.set_code(ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR);
        st.set_msg("failed to save tablet meta");
        return st;
    }

    st.set_code(MetaServiceCode::OK);
    st.set_msg("OK");
    return st;
}

void MetaServiceImpl::create_tablets(::google::protobuf::RpcController* controller,
                                    const ::selectdb::CreateTabletsRequest* request,
                                    ::selectdb::MetaServiceGenericResponse* response,
                                    ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request=" << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&ret, &code, &msg, &response, &ctrl](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << (ret == 0 ? "succ to " : "failed to ") << __PRETTY_FUNCTION__ << " "
                          << ctrl->remote_side() << " " << msg;
            });

    if (request->tablet_metas_size() == 0) {
        msg = "no tablet meta";
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }
    std::string instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }

    for (auto& tablet_meta : request->tablet_metas()) {
        auto& meta = const_cast<doris::TabletMetaPB&>(tablet_meta);
        MetaServiceResponseStatus st = internal_create_tablet(meta, txn_kv_, instance_id);
        if (st.code() != MetaServiceCode::OK) {
            ret = -1;
        }
    }
}

static MetaServiceResponseStatus s_get_tablet(const std::string& instance_id, Transaction* txn,
                                              int64_t tablet_id, doris::TabletMetaPB* tablet_meta) {
    MetaServiceResponseStatus st;
    int ret = 0;
    // TODO: validate request
    MetaTabletIdxKeyInfo key_info0 {instance_id, tablet_id};
    std::string key0, val0;
    meta_tablet_idx_key(key_info0, &key0);
    ret = txn->get(key0, &val0);
    LOG(INFO) << "get tablet meta, tablet_id=" << tablet_id << " key=" << hex(key0);
    if (ret != 0) {
        std::stringstream ss;
        ss << "failed to get table id from tablet_id, err="
           << (ret == 1 ? "not found" : "internal error");
        st.set_code(MetaServiceCode::KV_TXN_GET_ERR);
        st.set_msg(ss.str());
        return st;
    }

    TabletIndexPB tablet_table;
    if (!tablet_table.ParseFromString(val0)) {
        st.set_code(MetaServiceCode::PROTOBUF_PARSE_ERR);
        st.set_msg("malformed tablet table value");
        return st;
    }

    MetaTabletKeyInfo key_info1 {instance_id, tablet_table.table_id(), tablet_table.index_id(),
                                 tablet_table.partition_id(), tablet_id};
    std::string key1, val1;
    meta_tablet_key(key_info1, &key1);
    ret = txn->get(key1, &val1);
    if (ret != 0) {
        std::string msg = "failed to get tablet";
        msg += (ret == 1 ? ": not found" : "");
        st.set_code(MetaServiceCode::KV_TXN_GET_ERR);
        st.set_msg(std::move(msg));
        return st;
    }

    if (!tablet_meta->ParseFromString(val1)) {
        st.set_code(MetaServiceCode::PROTOBUF_PARSE_ERR);
        st.set_msg("malformed tablet meta, unable to initialize");
        return st;
    }

    st.set_code(MetaServiceCode::OK);
    st.set_msg("OK");
    return st;
}

void MetaServiceImpl::update_tablet(::google::protobuf::RpcController* controller,
                                    const ::selectdb::UpdateTabletRequest* request,
                                    ::selectdb::MetaServiceGenericResponse* response,
                                    ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request=" << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    MetaServiceResponseStatus status;
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&status, &response, &ctrl](int*) {
                LOG(INFO) << (status.code() == MetaServiceCode::OK ? "succ to " : "failed to ")
                          << __PRETTY_FUNCTION__ << " " << ctrl->remote_side() << " "
                          << status.msg() << " code=" << status.code();
                *response->mutable_status() = std::move(status);
            });
    std::string instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        status.set_code(MetaServiceCode::INVALID_ARGUMENT);
        status.set_msg("empty instance_id");
        LOG(INFO) << status.msg() << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    std::unique_ptr<Transaction> txn;
    if (txn_kv_->create_txn(&txn) != 0) {
        status.set_code(MetaServiceCode::KV_TXN_CREATE_ERR);
        status.set_msg("failed to init txn");
        return;
    }
    for (const TabletMetaInfoPB& tablet_meta_info : request->tablet_meta_infos()) {
        doris::TabletMetaPB tablet_meta;
        status = s_get_tablet(instance_id, txn.get(), tablet_meta_info.tablet_id(), &tablet_meta);
        if (status.code() != MetaServiceCode::OK) {
            return;
        }
        if (tablet_meta_info.has_is_in_memory()) {
            tablet_meta.set_is_in_memory(tablet_meta_info.is_in_memory());
        } else if (tablet_meta_info.has_is_persistent()) {
            tablet_meta.set_is_persistent(tablet_meta_info.is_persistent());
        }
        int64_t table_id = tablet_meta.table_id();
        int64_t index_id = tablet_meta.index_id();
        int64_t partition_id = tablet_meta.partition_id();
        int64_t tablet_id = tablet_meta.tablet_id();

        MetaTabletKeyInfo key_info {instance_id, table_id, index_id, partition_id, tablet_id};
        std::string key;
        std::string val;
        meta_tablet_key(key_info, &key);
        if (!tablet_meta.SerializeToString(&val)) {
            status.set_code(MetaServiceCode::PROTOBUF_SERIALIZE_ERR);
            status.set_msg("failed to serialize tablet meta");
            return;
        }
        txn->put(key, val);
        LOG(INFO) << "xxx put tablet_key=" << hex(key);
    }
    if (txn->commit() != 0) {
        status.set_code(MetaServiceCode::KV_TXN_COMMIT_ERR);
        status.set_msg("failed to update tablet meta");
        return;
    }
}

void MetaServiceImpl::get_tablet(::google::protobuf::RpcController* controller,
                                 const ::selectdb::GetTabletRequest* request,
                                 ::selectdb::GetTabletResponse* response,
                                 ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request=" << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    MetaServiceResponseStatus status;
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&status, &response, &ctrl](int*) {
                LOG(INFO) << (status.code() == MetaServiceCode::OK ? "succ to " : "failed to ")
                          << __PRETTY_FUNCTION__ << " " << ctrl->remote_side() << " "
                          << status.msg() << " code=" << status.code();
                *response->mutable_status() = std::move(status);
            });
    std::string instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        status.set_code(MetaServiceCode::INVALID_ARGUMENT);
        status.set_msg("empty instance_id");
        LOG(INFO) << status.msg() << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }

    std::unique_ptr<Transaction> txn;
    if (txn_kv_->create_txn(&txn) != 0) {
        status.set_code(MetaServiceCode::KV_TXN_CREATE_ERR);
        status.set_msg("failed to init txn");
        return;
    }
    status = s_get_tablet(instance_id, txn.get(), request->tablet_id(),
                          response->mutable_tablet_meta());
}

/**
 * 0. Construct the corresponding rowset commit_key according to the info in request
 * 1. Check whether this rowset has already been committed through commit_key
 *     a. if has been committed, abort prepare_rowset 
 *     b. else, goto 2
 * 2. Construct recycle rowset kv which contains object path
 * 3. Put recycle rowset kv
 */
void MetaServiceImpl::prepare_rowset(::google::protobuf::RpcController* controller,
                                     const ::selectdb::CreateRowsetRequest* request,
                                     ::selectdb::CreateRowsetResponse* response,
                                     ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request=" << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&code, &msg, &response, &ctrl](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << (code == MetaServiceCode::OK ? "succ to " : "failed to ")
                          << __PRETTY_FUNCTION__ << " " << ctrl->remote_side() << " " << msg;
            });
    if (!request->has_rowset_meta()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no rowset meta";
        return;
    }
    std::string instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    // temporary == true is for loading rowset from user,
    // temporary == false is for doris internal rowset put, such as data conversion in schema change procedure.
    bool temporary = request->has_temporary() ? request->temporary() : false;
    int64_t tablet_id = request->rowset_meta().tablet_id();
    int64_t end_version = request->rowset_meta().end_version();
    const auto& rowset_id = request->rowset_meta().rowset_id_v2();

    std::string commit_key;
    std::string commit_val;

    if (temporary) {
        int64_t txn_id = request->rowset_meta().txn_id();
        MetaRowsetTmpKeyInfo key_info {instance_id, txn_id, tablet_id};
        meta_rowset_tmp_key(key_info, &commit_key);
    } else {
        MetaRowsetKeyInfo key_info {instance_id, tablet_id, end_version};
        meta_rowset_key(key_info, &commit_key);
    }

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }

    // Check if commit key already exists.
    ret = txn->get(commit_key, &commit_val);
    if (ret == 0) {
        if (!response->mutable_existed_rowset_meta()->ParseFromString(commit_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = fmt::format("malformed rowset meta value. key={}", hex(commit_key));
            return;
        }
        code = MetaServiceCode::ALREADY_EXISTED;
        msg = "rowset already exists";
        return;
    }
    if (ret != 1) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        msg = "failed to check whether rowset exists";
        return;
    }

    std::string prepare_key;
    std::string prepare_val;
    RecycleRowsetKeyInfo prepare_key_info {instance_id, tablet_id, rowset_id};
    recycle_rowset_key(prepare_key_info, &prepare_key);
    RecycleRowsetPB prepare_rowset;
    prepare_rowset.set_tablet_id(request->rowset_meta().tablet_id());
    prepare_rowset.set_resource_id(request->rowset_meta().resource_id());
    prepare_rowset.set_creation_time(request->rowset_meta().creation_time());
    prepare_rowset.SerializeToString(&prepare_val);

    txn->put(prepare_key, prepare_val);
    LOG(INFO) << "xxx put" << (temporary ? " tmp " : " ") << "prepare_rowset_key "
              << hex(prepare_key);
    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = "failed to save recycle rowset";
        return;
    }
}

/**
 * 0. Construct the corresponding rowset commit_key and commit_value according
 *    to the info in request
 * 1. Check whether this rowset has already been committed through commit_key
 *     a. if has been committed
 *         1. if committed value is same with commit_value, it may be a redundant
 *            retry request, return ok
 *         2. else, abort commit_rowset 
 *     b. else, goto 2
 * 2. Construct the corresponding rowset prepare_key(recycle rowset)
 * 3. Remove prepare_key and put commit rowset kv
 */
void MetaServiceImpl::commit_rowset(::google::protobuf::RpcController* controller,
                                    const ::selectdb::CreateRowsetRequest* request,
                                    ::selectdb::CreateRowsetResponse* response,
                                    ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request=" << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&code, &msg, &response, &ctrl](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << (code == MetaServiceCode::OK ? "succ to " : "failed to ")
                          << __PRETTY_FUNCTION__ << " " << ctrl->remote_side() << " " << msg;
            });
    if (!request->has_rowset_meta()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no rowset meta";
        return;
    }
    std::string instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    // temporary == true is for loading rowset from user,
    // temporary == false is for doris internal rowset put, such as data conversion in schema change procedure.
    bool temporary = request->has_temporary() ? request->temporary() : false;
    int64_t tablet_id = request->rowset_meta().tablet_id();
    int64_t end_version = request->rowset_meta().end_version();
    const auto& rowset_id = request->rowset_meta().rowset_id_v2();

    std::string commit_key;
    std::string commit_val;

    if (temporary) { // Txn
        int64_t txn_id = request->rowset_meta().txn_id();
        MetaRowsetTmpKeyInfo key_info {instance_id, txn_id, tablet_id};
        meta_rowset_tmp_key(key_info, &commit_key);
    } else { // Schema change
        MetaRowsetKeyInfo key_info {instance_id, tablet_id, end_version};
        meta_rowset_key(key_info, &commit_key);
    }

    if (!request->rowset_meta().SerializeToString(&commit_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        msg = "failed to serialize rowset meta";
        return;
    }

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }

    // Check if commit key already exists.
    std::string existed_commit_val;
    ret = txn->get(commit_key, &existed_commit_val);
    if (ret == 0) {
        if (existed_commit_val == commit_val) {
            // Same request, return OK
            return;
        }
        if (!response->mutable_existed_rowset_meta()->ParseFromString(commit_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = fmt::format("malformed rowset meta value. key={}", hex(commit_key));
            return;
        }
        code = MetaServiceCode::ALREADY_EXISTED;
        msg = "rowset already exists";
        return;
    }
    if (ret != 1) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        msg = "failed to check whether rowset exists";
        return;
    }

    std::string prepare_key;
    RecycleRowsetKeyInfo prepare_key_info {instance_id, tablet_id, rowset_id};
    recycle_rowset_key(prepare_key_info, &prepare_key);

    if (!request->rowset_meta().SerializeToString(&commit_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        msg = "failed to serialize rowset meta";
        return;
    }

    txn->remove(prepare_key);
    txn->put(commit_key, commit_val);
    LOG(INFO) << "xxx put" << (temporary ? " tmp " : " ") << "commit_rowset_key "
              << hex(prepare_key);
    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = "failed to save rowset meta";
        return;
    }
}

static void s_get_rowset(Transaction* txn, int64_t start, int64_t end,
                         const std::string& instance_id, int64_t tablet_id, int& ret,
                         MetaServiceCode& code, std::string& msg,
                         ::selectdb::GetRowsetResponse* response) {
    LOG(INFO) << "s_get_rowset start=" << start << ", end=" << end;
    MetaRowsetKeyInfo key_info0 {instance_id, tablet_id, start};
    MetaRowsetKeyInfo key_info1 {instance_id, tablet_id, end + 1};
    std::string key0;
    std::string key1;
    meta_rowset_key(key_info0, &key0);
    meta_rowset_key(key_info1, &key1);
    std::unique_ptr<RangeGetIterator> it;

    int num_rowsets = 0;
    std::unique_ptr<int, std::function<void(int*)>> defer_log_range(
            (int*)0x01, [key0, key1, &num_rowsets](int*) {
                LOG(INFO) << "get rowset meta, num_rowsets=" << num_rowsets << " range=["
                          << hex(key0) << "," << hex(key1) << "]";
            });

    std::stringstream ss;
    do {
        ret = txn->get(key0, key1, &it);
        if (ret != 0) {
            code = MetaServiceCode::KV_TXN_GET_ERR;
            ss << "internal error, failed to get rowset, ret=" << ret;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }

        while (it->has_next()) {
            auto [k, v] = it->next();
            LOG(INFO) << "xxx range get rowset_key=" << hex(k);
            auto rs = response->add_rowset_meta();
            if (!rs->ParseFromArray(v.data(), v.size())) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = "malformed rowset meta, unable to deserialize";
                LOG(WARNING) << msg << " key=" << hex(k);
                ret = -1;
                return;
            }
            ++num_rowsets;
            if (!it->has_next()) key0 = k;
        }
        key0.push_back('\x00'); // Update to next smallest key for iteration
    } while (it->more());
}

std::vector<std::pair<int64_t, int64_t>> MetaServiceImpl::calc_sync_versions(
        int64_t req_bc_cnt, int64_t bc_cnt, int64_t req_cc_cnt, int64_t cc_cnt, int64_t req_cp,
        int64_t cp, int64_t req_start, int64_t req_end) {
    using Version = std::pair<int64_t, int64_t>;
    // combine `v1` `v2`  to `v1`, return true if success
    static auto combine_if_overlapping = [](Version& v1, Version& v2) -> bool {
        if (v1.second + 1 < v2.first || v2.second + 1 < v1.first) return false;
        v1.first = std::min(v1.first, v2.first);
        v1.second = std::max(v1.second, v2.second);
        return true;
    };
    // [xxx]: compacted versions
    // ^~~~~: cumulative point
    // ^___^: related versions
    std::vector<Version> versions;
    if (req_bc_cnt < bc_cnt) {
        // * for any BC happended
        // BE  [=][=][=][=][=====][=][=]
        //                  ^~~~~ req_cp
        // MS  [xxxxxxxxxx][xxxxxxxxxxxxxx][=======][=][=]
        //                                  ^~~~~~~ ms_cp
        //     ^_________________________^ versions_return: [0, ms_cp - 1]
        versions.emplace_back(0, cp - 1);
    }

    if (req_cc_cnt < cc_cnt) {
        Version cc_version;
        if (req_cp < cp) {
            // * only one CC happened and CP changed
            // BE  [=][=][=][=][=====][=][=]
            //                  ^~~~~ req_cp
            // MS  [=][=][=][=][xxxxxxxxxxxxxx][=======][=][=]
            //                                  ^~~~~~~ ms_cp
            //                  ^____________^ related_versions: [req_cp, ms_cp - 1]
            //
            // * more than one CC happened and CP changed
            // BE  [=][=][=][=][=====][=][=]
            //                  ^~~~~ req_cp
            // MS  [=][=][=][=][xxxxxxxxxxxxxx][xxxxxxx][=][=]
            //                                           ^~~~~~~ ms_cp
            //                  ^_____________________^ related_versions: [req_cp, ms_cp - 1]
            cc_version = {req_cp, cp - 1};
        } else {
            // * more than one CC happened and CP remain unchanged
            // BE  [=][=][=][=][=====][=][=]
            //                  ^~~~~ req_cp
            // MS  [=][=][=][=][xxxxxxxxxxxxxx][xxxxxxx][=][=]
            //                  ^~~~~~~~~~~~~~ ms_cp
            //                  ^_____________________^ related_versions: [req_cp, max]
            //                                           there may be holes if we don't return all version
            //                                           after ms_cp, however it can be optimized.
            cc_version = {req_cp, std::numeric_limits<int64_t>::max() - 1};
        }
        if (versions.empty() || !combine_if_overlapping(versions.front(), cc_version)) {
            versions.push_back(cc_version);
        }
    }

    Version query_version {req_start, req_end};
    bool combined = false;
    for (auto& v : versions) {
        if ((combined = combine_if_overlapping(v, query_version))) break;
    }
    if (!combined) {
        versions.push_back(query_version);
    }
    std::sort(versions.begin(), versions.end(),
              [](const Version& v1, const Version& v2) { return v1.first < v2.first; });
    return versions;
}

void MetaServiceImpl::get_rowset(::google::protobuf::RpcController* controller,
                                 const ::selectdb::GetRowsetRequest* request,
                                 ::selectdb::GetRowsetResponse* response,
                                 ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request: " << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    std::stringstream ss;
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&code, &msg, &response, &ctrl](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << (code == MetaServiceCode::OK ? "succ to " : "failed to ")
                          << __PRETTY_FUNCTION__ << " " << ctrl->remote_side() << " " << msg;
            });

    std::string instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }

    int64_t tablet_id = request->idx().has_tablet_id() ? request->idx().tablet_id() : -1;
    if (tablet_id <= 0) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no valid tablet_id given";
        return;
    }

    if (!request->has_base_compaction_cnt() || !request->has_cumulative_compaction_cnt() ||
        !request->has_cumulative_point()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no valid compaction_cnt or cumulative_point given";
        return;
    }
    int64_t req_bc_cnt = request->base_compaction_cnt();
    int64_t req_cc_cnt = request->cumulative_compaction_cnt();
    int64_t req_cp = request->cumulative_point();

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }

    int64_t table_id = request->idx().table_id();
    int64_t index_id = request->idx().index_id();
    int64_t partition_id = request->idx().partition_id();
    // Get tablet id index from kv
    if (table_id <= 0 || index_id <= 0 || partition_id <= 0) {
        std::string idx_key = meta_tablet_idx_key({instance_id, tablet_id});
        std::string idx_val;
        ret = txn->get(idx_key, &idx_val);
        LOG(INFO) << "get tablet meta, tablet_id=" << tablet_id << " key=" << hex(idx_key);
        if (ret != 0) {
            ss << "failed to get table id from tablet_id, err="
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

    std::string tablet_stat_key;
    std::string tablet_stat_val;
    StatsTabletKeyInfo key_info {instance_id, table_id, index_id, partition_id, tablet_id};
    stats_tablet_key(key_info, &tablet_stat_key);
    ret = txn->get(tablet_stat_key, &tablet_stat_val);
    LOG(INFO) << "get tablet stats, tablet_id=" << tablet_id << " key=" << hex(tablet_stat_key);
    if (ret != 0) {
        code = ret == 1 ? MetaServiceCode::TABLET_NOT_FOUND : MetaServiceCode::KV_TXN_GET_ERR;
        ss << (ret == 1 ? " not_found" : " failed") << "_tablet_id=" << tablet_id;
        msg = ss.str();
        LOG(WARNING) << "failed to get tablet stats, tablet_id=" << tablet_id << " ret=" << ret
                     << " key=" << hex(tablet_stat_key);
        return;
    }
    TabletStatsPB tablet_stat;
    if (!tablet_stat.ParseFromString(tablet_stat_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "malformed tablet stat value";
        return;
    }
    VLOG_DEBUG << "tablet_id=" << tablet_id << " stats=" << proto_to_json(tablet_stat);

    int64_t bc_cnt = tablet_stat.base_compaction_cnt();
    int64_t cc_cnt = tablet_stat.cumulative_compaction_cnt();
    int64_t cp = tablet_stat.cumulative_point();

    response->mutable_stats()->CopyFrom(tablet_stat);

    int64_t req_start = request->start_version();
    int64_t req_end = request->end_version();
    req_end = req_end < 0 ? std::numeric_limits<int64_t>::max() - 1 : req_end;

    LOG(INFO) << "req_bc_cnt=" << req_bc_cnt << ", bc_cnt=" << bc_cnt
              << ", req_cc_cnt=" << req_cc_cnt << ", cc_cnt=" << cc_cnt << ", req_cp=" << req_cp
              << ", cp=" << cp;
    //==========================================================================
    //      Find version ranges to be synchronized due to compaction
    //==========================================================================
    if (req_bc_cnt > bc_cnt || req_cc_cnt > cc_cnt || req_cp > cp) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "no valid compaction_cnt or cumulative_point given. req_bc_cnt=" << req_bc_cnt
           << ", bc_cnt=" << bc_cnt << ", req_cc_cnt=" << req_cc_cnt << ", cc_cnt=" << cc_cnt
           << ", req_cp=" << req_cp << ", cp=" << cp;
        msg = ss.str();
        return;
    }
    auto versions = calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt, req_cp, cp,
                                       req_start, req_end);
    for (auto [start, end] : versions) {
        s_get_rowset(txn.get(), start, end, instance_id, tablet_id, ret, code, msg, response);
        if (ret != 0) {
            return;
        }
    }
}

void MetaServiceImpl::prepare_index(::google::protobuf::RpcController* controller,
                                    const ::selectdb::IndexRequest* request,
                                    ::selectdb::MetaServiceGenericResponse* response,
                                    ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "prepare_index rpc from " << ctrl->remote_side()
              << " request: " << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&response, &ctrl](int*) {
                LOG(INFO) << (response->status().code() == 0 ? "succ to " : "failed to ")
                          << __PRETTY_FUNCTION__ << " " << ctrl->remote_side() << " "
                          << response->status().msg();
            });
    int ret = index_exists(request, response);
    if (ret < 0) {
        return;
    } else if (ret == 0) {
        response->mutable_status()->set_code(MetaServiceCode::ALREADY_EXISTED);
        response->mutable_status()->set_msg("index already existed");
        return;
    }
    put_recycle_index_kv(request, response);
}

void MetaServiceImpl::commit_index(::google::protobuf::RpcController* controller,
                                   const ::selectdb::IndexRequest* request,
                                   ::selectdb::MetaServiceGenericResponse* response,
                                   ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "commit_index rpc from " << ctrl->remote_side()
              << " request: " << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    remove_recycle_index_kv(request, response);
    LOG(INFO) << (response->status().code() == 0 ? "succ to " : "failed to ") << __PRETTY_FUNCTION__
              << " " << ctrl->remote_side() << " " << response->status().msg();
}

void MetaServiceImpl::drop_index(::google::protobuf::RpcController* controller,
                                 const ::selectdb::IndexRequest* request,
                                 ::selectdb::MetaServiceGenericResponse* response,
                                 ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "drop_index rpc from " << ctrl->remote_side()
              << " request: " << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    put_recycle_index_kv(request, response);
    LOG(INFO) << (response->status().code() == 0 ? "succ to " : "failed to ") << __PRETTY_FUNCTION__
              << " " << ctrl->remote_side() << " " << response->status().msg();
}

int MetaServiceImpl::index_exists(const ::selectdb::IndexRequest* request,
                                  ::selectdb::MetaServiceGenericResponse* response) {
    std::string instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("empty instance_id");
        return -1;
    }
    const auto& index_ids = request->index_ids();
    if (index_ids.empty() || !request->has_table_id()) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("empty index_ids or table_id");
        return -1;
    }
    MetaTabletKeyInfo info0 {instance_id, request->table_id(), index_ids[0], 0, 0};
    MetaTabletKeyInfo info1 {instance_id, request->table_id(), index_ids[0],
                             std::numeric_limits<int64_t>::max(), 0};
    std::string key0;
    std::string key1;
    meta_tablet_key(info0, &key0);
    meta_tablet_key(info1, &key1);

    std::unique_ptr<RangeGetIterator> it;
    std::unique_ptr<Transaction> txn;
    int ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_CREATE_ERR);
        response->mutable_status()->set_msg("empty index_ids or table_id");
        LOG(WARNING) << "failed to create txn";
        return -1;
    }
    ret = txn->get(key0, key1, &it, 1);
    if (ret != 0) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_GET_ERR);
        response->mutable_status()->set_msg("failed to get tablet when checking index existence");
        return -1;
    }
    if (!it->has_next()) {
        return 1;
    }
    return 0;
}

void MetaServiceImpl::put_recycle_index_kv(const ::selectdb::IndexRequest* request,
                                           ::selectdb::MetaServiceGenericResponse* response) {
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&code, &msg, &response](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
            });

    std::string instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    const auto& index_ids = request->index_ids();
    if (index_ids.empty() || !request->has_table_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty index_ids or table_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }

    int64_t creation_time = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();

    std::vector<std::pair<std::string, std::string>> kvs;
    kvs.reserve(index_ids.size());

    for (int64_t index_id : index_ids) {
        std::string key;
        RecycleIndexKeyInfo key_info {instance_id, index_id};
        recycle_index_key(key_info, &key);

        RecycleIndexPB recycle_index;
        recycle_index.set_table_id(request->table_id());
        recycle_index.set_creation_time(creation_time);
        std::string val = recycle_index.SerializeAsString();

        kvs.emplace_back(std::move(key), std::move(val));
    }

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }

    for (const auto& [k, v] : kvs) {
        txn->put(k, v);
    }

    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = "failed to save recycle index kv";
        return;
    }
}

void MetaServiceImpl::remove_recycle_index_kv(const ::selectdb::IndexRequest* request,
                                              ::selectdb::MetaServiceGenericResponse* response) {
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&code, &msg, &response](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
            });

    std::string instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    const auto& index_ids = request->index_ids();
    if (index_ids.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty index_ids";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }

    std::vector<std::string> keys;
    keys.reserve(index_ids.size());

    for (int64_t index_id : index_ids) {
        std::string key;
        RecycleIndexKeyInfo key_info {instance_id, index_id};
        recycle_index_key(key_info, &key);
        keys.push_back(std::move(key));
    }

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }

    for (const auto& k : keys) {
        txn->remove(k);
    }

    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = "failed to remove recycle index kv";
        return;
    }
}

void MetaServiceImpl::prepare_partition(::google::protobuf::RpcController* controller,
                                        const ::selectdb::PartitionRequest* request,
                                        ::selectdb::MetaServiceGenericResponse* response,
                                        ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "prepare_partition rpc from " << ctrl->remote_side()
              << " request: " << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&response, &ctrl](int*) {
                LOG(INFO) << (response->status().code() == 0 ? "succ to " : "failed to ")
                          << __PRETTY_FUNCTION__ << " " << ctrl->remote_side() << " "
                          << response->status().msg();
            });
    int ret = partition_exists(request, response);
    if (ret < 0) {
        return;
    } else if (ret == 0) {
        response->mutable_status()->set_code(MetaServiceCode::ALREADY_EXISTED);
        response->mutable_status()->set_msg("partition already existed");
        return;
    }
    put_recycle_partition_kv(request, response);
}

void MetaServiceImpl::commit_partition(::google::protobuf::RpcController* controller,
                                       const ::selectdb::PartitionRequest* request,
                                       ::selectdb::MetaServiceGenericResponse* response,
                                       ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "commit_partition rpc from " << ctrl->remote_side()
              << " request: " << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    remove_recycle_partition_kv(request, response);
    LOG(INFO) << (response->status().code() == 0 ? "succ to " : "failed to ") << __PRETTY_FUNCTION__
              << " " << ctrl->remote_side() << " " << response->status().msg();
}

void MetaServiceImpl::drop_partition(::google::protobuf::RpcController* controller,
                                     const ::selectdb::PartitionRequest* request,
                                     ::selectdb::MetaServiceGenericResponse* response,
                                     ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "drop_partition rpc from " << ctrl->remote_side()
              << " request: " << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    put_recycle_partition_kv(request, response);
    LOG(INFO) << (response->status().code() == 0 ? "succ to " : "failed to ") << __PRETTY_FUNCTION__
              << " " << ctrl->remote_side() << " " << response->status().msg();
}

int MetaServiceImpl::partition_exists(const ::selectdb::PartitionRequest* request,
                                      ::selectdb::MetaServiceGenericResponse* response) {
    std::string instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("empty instance_id");
        return -1;
    }
    const auto& index_ids = request->index_ids();
    const auto& partition_ids = request->partition_ids();
    if (partition_ids.empty() || index_ids.empty() || !request->has_table_id()) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("empty partition_ids or index_ids or table_id");
        return -1;
    }
    MetaTabletKeyInfo info0 {instance_id, request->table_id(), index_ids[0], partition_ids[0], 0};
    MetaTabletKeyInfo info1 {instance_id, request->table_id(), index_ids[0], partition_ids[0],
                             std::numeric_limits<int64_t>::max()};
    std::string key0;
    std::string key1;
    meta_tablet_key(info0, &key0);
    meta_tablet_key(info1, &key1);

    std::unique_ptr<RangeGetIterator> it;
    std::unique_ptr<Transaction> txn;
    int ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_CREATE_ERR);
        response->mutable_status()->set_msg("failed to create txn");
        return -1;
    }
    ret = txn->get(key0, key1, &it, 1);
    if (ret != 0) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_GET_ERR);
        response->mutable_status()->set_msg(
                "failed to get tablet when checking partition existence");
        return -1;
    }
    if (!it->has_next()) {
        return 1;
    }
    return 0;
}

void MetaServiceImpl::put_recycle_partition_kv(const ::selectdb::PartitionRequest* request,
                                               ::selectdb::MetaServiceGenericResponse* response) {
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&code, &msg, &response](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
            });

    std::string instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    const auto& partition_ids = request->partition_ids();
    const auto& index_ids = request->index_ids();
    if (partition_ids.empty() || index_ids.empty() || !request->has_table_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty partition_ids or index_ids or table_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }

    int64_t creation_time = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();

    std::vector<std::pair<std::string, std::string>> kvs;
    kvs.reserve(partition_ids.size());

    for (int64_t partition_id : partition_ids) {
        std::string key;
        RecyclePartKeyInfo key_info {instance_id, partition_id};
        recycle_partition_key(key_info, &key);

        RecyclePartitionPB recycle_partition;
        recycle_partition.set_table_id(request->table_id());
        *recycle_partition.mutable_index_id() = index_ids;
        recycle_partition.set_creation_time(creation_time);
        std::string val = recycle_partition.SerializeAsString();

        kvs.emplace_back(std::move(key), std::move(val));
    }

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }

    for (const auto& [k, v] : kvs) {
        txn->put(k, v);
    }

    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = "failed to save recycle partition kv";
        return;
    }
}

void MetaServiceImpl::remove_recycle_partition_kv(
        const ::selectdb::PartitionRequest* request,
        ::selectdb::MetaServiceGenericResponse* response) {
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&code, &msg, &response](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
            });

    std::string instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    const auto& partition_ids = request->partition_ids();
    if (partition_ids.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty partition_ids";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }

    std::vector<std::string> keys;
    keys.reserve(partition_ids.size());

    for (int64_t partition_id : partition_ids) {
        std::string key;
        RecyclePartKeyInfo key_info {instance_id, partition_id};
        recycle_partition_key(key_info, &key);
        keys.push_back(std::move(key));
    }

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }

    for (const auto& k : keys) {
        txn->remove(k);
    }

    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = "failed to remove recycle partition kv";
        return;
    }
}

void MetaServiceImpl::get_tablet_stats(::google::protobuf::RpcController* controller,
                                       const ::selectdb::GetTabletStatsRequest* request,
                                       ::selectdb::GetTabletStatsResponse* response,
                                       ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " rpc=MetaServiceImpl::get_tablet_stats";
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&ret, &code, &msg, &response, &ctrl](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << (ret == 0 ? "succ to " : "failed to ") << __PRETTY_FUNCTION__ << " "
                          << ctrl->remote_side() << " " << msg;
            });

    std::string instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }

    std::stringstream ss;
    for (auto& i : request->tablet_idx()) {
        if (!(/* i.has_db_id() && */ i.has_table_id() && i.has_index_id() && i.has_partition_id() &&
              i.has_tablet_id())) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            ss << " incomplete tablet_idx";
            LOG(WARNING) << "incomplete index for tablet stats, tablet_idx=" << i.DebugString();
            continue;
        }
        StatsTabletKeyInfo stat_key_info {instance_id, i.table_id(), i.index_id(), i.partition_id(),
                                          i.tablet_id()};
        std::string key, val;
        stats_tablet_key(stat_key_info, &key);
        // TODO(gavin): make the txn live longer
        std::unique_ptr<Transaction> txn;
        ret = txn_kv_->create_txn(&txn);
        if (ret != 0) {
            code = MetaServiceCode::KV_TXN_CREATE_ERR;
            ss << " failed_create_txn_tablet=" << i.tablet_id();
            LOG(WARNING) << "failed to create txn"
                         << " ret=" << ret << " key=" << hex(key);
            continue;
        }
        ret = txn->get(key, &val);
        if (ret != 0) { // Try the best to return as much info as it can
            code = MetaServiceCode::KV_TXN_GET_ERR;
            ss << (ret == 1 ? " not_found" : " failed") << "_tablet_id=" << i.tablet_id();
            LOG(WARNING) << "failed to get tablet stats, tablet_id=" << i.tablet_id()
                         << " ret=" << ret << " key=" << hex(key);
            continue;
        }
        if (!response->add_tablet_stats()->ParseFromString(val)) {
            LOG(WARNING) << "failed to parse tablet stats pb, tablet_id=" << i.tablet_id();
        }
    }
    msg = ss.str();
}

std::string static convert_ms_code_to_http_code(const MetaServiceCode& ret, int& status_code) {
    switch (ret) {
    case OK:
        return "OK";
        break;
    case INVALID_ARGUMENT:
    case PROTOBUF_PARSE_ERR:
        status_code = 400;
        return "INVALID_ARGUMENT";
        break;
    case KV_TXN_CREATE_ERR:
    case KV_TXN_GET_ERR:
    case KV_TXN_COMMIT_ERR:
    case PROTOBUF_SERIALIZE_ERR:
    case TXN_GEN_ID_ERR:
    case TXN_DUPLICATED_REQ:
    case TXN_LABEL_ALREADY_USED:
    case TXN_INVALID_STATUS:
    case TXN_LABEL_NOT_FOUND:
    case TXN_ID_NOT_FOUND:
    case TXN_ALREADY_ABORTED:
    case TXN_ALREADY_VISIBLE:
    case TXN_ALREADY_PRECOMMITED:
    case VERSION_NOT_FOUND:
    case UNDEFINED_ERR:
        status_code = 500;
        return "INTERANAL_ERROR";
        break;
    case CLUSTER_NOT_FOUND:
        status_code = 404;
        return "NOT_FOUND";
        break;
    case ALREADY_EXISTED:
        status_code = 409;
        return "ALREADY_EXISTED";
        break;
    default:
        status_code = 500;
        return "INTERANAL_ERROR";
        break;
    }
}

void static format_to_json_resp(std::string& response_body, const MetaServiceCode& ret,
                                const std::string& c_ret, const std::string& msg) {
    rapidjson::Document d;
    d.Parse(response_body.c_str());
    rapidjson::StringBuffer sb;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(sb);
    writer.StartObject();
    writer.Key("code");
    writer.String(d.HasParseError() ? c_ret.c_str() : "OK");
    writer.Key("msg");
    writer.String(ret == MetaServiceCode::OK ? "" : msg.c_str());
    writer.EndObject();
    if (!d.HasParseError()) {
        // json string, add result obj into it.
        rapidjson::Document d2;
        d2.Parse(sb.GetString());
        d2.AddMember("result", d, d2.GetAllocator());
        sb.Clear();
        writer.Reset(sb);
        d2.Accept(writer);
    }
    response_body = sb.GetString();
}

void MetaServiceImpl::http(::google::protobuf::RpcController* controller,
                           const ::selectdb::MetaServiceHttpRequest* request,
                           ::selectdb::MetaServiceHttpResponse* response,
                           ::google::protobuf::Closure* done) {
    auto cntl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << cntl->remote_side() << " request: " << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    MetaServiceCode ret = MetaServiceCode::OK;
    int status_code = 200;
    std::string msg = "OK";
    std::string req;
    std::string response_body;
    std::string request_body;
    bool keep_raw_body = false;
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01,
            [&ret, &msg, &status_code, &response_body, &cntl, &req, &keep_raw_body](int*) {
                std::string c_ret = convert_ms_code_to_http_code(ret, status_code);
                LOG(INFO) << (ret == 0 ? "succ to " : "failed to ") << __PRETTY_FUNCTION__ << " "
                          << cntl->remote_side() << " request=\n"
                          << req << "\n ret=" << ret << " msg=" << msg;
                cntl->http_response().set_status_code(status_code);
                if (!keep_raw_body) format_to_json_resp(response_body, ret, c_ret, msg);
                cntl->response_attachment().append(response_body);
                cntl->response_attachment().append("\n");
            });

    // Prepare input request info
    auto unresolved_path = cntl->http_request().unresolved_path();
    auto uri = cntl->http_request().uri();
    std::stringstream ss;
    ss << "\nuri_path=" << uri.path();
    ss << "\nunresolved_path=" << unresolved_path;
    ss << "\nmethod=" << brpc::HttpMethod2Str(cntl->http_request().method());
    ss << "\nquery strings:";
    for (auto it = uri.QueryBegin(); it != uri.QueryEnd(); ++it) {
        ss << "\n" << it->first << "=" << it->second;
    }
    ss << "\nheaders:";
    for (auto it = cntl->http_request().HeaderBegin(); it != cntl->http_request().HeaderEnd();
         ++it) {
        ss << "\n" << it->first << ":" << it->second;
    }
    req = ss.str();
    ss.clear();
    ss.str("");
    request_body = cntl->request_attachment().to_string(); // Just copy

    // Auth
    auto token = uri.GetQuery("token");
    if (token == nullptr || *token != config::http_token) {
        msg = "incorrect token, token=" + (token == nullptr ? std::string("(not given)") : *token);
        response_body = "incorrect token";
        status_code = 403;
        return;
    }

    // Process http request
    // just for debug and regression test
    if (unresolved_path == "get_obj_store_info") {
        GetObjStoreInfoRequest r;
        auto st = google::protobuf::util::JsonStringToMessage(request_body, &r);
        if (!st.ok()) {
            msg = "failed to GetObjStoreInfoRequest, error: " + st.message().ToString();
            ret = MetaServiceCode::PROTOBUF_PARSE_ERR;
            response_body = msg;
            LOG(WARNING) << msg;
            return;
        }
        GetObjStoreInfoResponse res;
        get_obj_store_info(cntl, &r, &res, nullptr);
        ret = res.status().code();
        msg = res.status().msg();
        response_body = msg;
        return;
    }
    if (unresolved_path == "update_ak_sk") {
        AlterObjStoreInfoRequest r;
        auto st = google::protobuf::util::JsonStringToMessage(request_body, &r);
        if (!st.ok()) {
            msg = "failed to SetObjStoreInfoRequest, error: " + st.message().ToString();
            ret = MetaServiceCode::PROTOBUF_PARSE_ERR;
            response_body = msg;
            LOG(WARNING) << msg;
            return;
        }
        r.set_op(AlterObjStoreInfoRequest::UPDATE_AK_SK);
        MetaServiceGenericResponse res;
        alter_obj_store_info(cntl, &r, &res, nullptr);
        ret = res.status().code();
        msg = res.status().msg();
        response_body = msg;
        return;
    }
    if (unresolved_path == "add_obj_info") {
        AlterObjStoreInfoRequest r;
        auto st = google::protobuf::util::JsonStringToMessage(request_body, &r);
        if (!st.ok()) {
            msg = "failed to SetObjStoreInfoRequest, error: " + st.message().ToString();
            ret = MetaServiceCode::PROTOBUF_PARSE_ERR;
            response_body = msg;
            LOG(WARNING) << msg;
            return;
        }
        r.set_op(AlterObjStoreInfoRequest::ADD_OBJ_INFO);
        MetaServiceGenericResponse res;
        alter_obj_store_info(cntl, &r, &res, nullptr);
        ret = res.status().code();
        msg = res.status().msg();
        response_body = msg;
        return;
    }
    if (unresolved_path == "decode_key") { // TODO: implement this in a separate src file
        if (uri.GetQuery("key") == nullptr || uri.GetQuery("key")->empty()) {
            msg = "no key to decode";
            response_body = msg;
            status_code = 400;
            return;
        }
        bool unicode = true;
        if (uri.GetQuery("unicode") != nullptr && *uri.GetQuery("unicode") == "false") {
            unicode = false;
        }
        std::string_view key = *uri.GetQuery("key");
        response_body = prettify_key(key, unicode);
        if (key.empty()) {
            msg = "failed to decode key, key=" + std::string(key);
            response_body = "failed to decode key, it may be malformed";
            status_code = 400;
            return;
        }
        keep_raw_body = true;
        return;
    }

    if (unresolved_path == "create_instance") {
        CreateInstanceRequest r;
        auto st = google::protobuf::util::JsonStringToMessage(request_body, &r);
        if (!st.ok()) {
            msg = "failed to CreateInstanceRequest, error: " + st.message().ToString();
            ret = MetaServiceCode::PROTOBUF_PARSE_ERR;
            response_body = msg;
            LOG(WARNING) << msg;
            return;
        }
        MetaServiceGenericResponse res;
        create_instance(cntl, &r, &res, nullptr);
        ret = res.status().code();
        msg = res.status().msg();
        response_body = msg;
        return;
    }

    if (unresolved_path == "drop_instance") {
        AlterInstanceRequest r;
        auto st = google::protobuf::util::JsonStringToMessage(request_body, &r);
        if (!st.ok()) {
            msg = "failed to AlterInstanceRequest, error: " + st.message().ToString();
            ret = MetaServiceCode::PROTOBUF_PARSE_ERR;
            response_body = msg;
            LOG(WARNING) << msg;
            return;
        }
        r.set_op(AlterInstanceRequest::DROP);
        MetaServiceGenericResponse res;
        alter_instance(cntl, &r, &res, nullptr);
        ret = res.status().code();
        msg = res.status().msg();
        response_body = msg;
        return;
    }

    if (unresolved_path == "add_cluster") {
        AlterClusterRequest r;
        auto st = google::protobuf::util::JsonStringToMessage(request_body, &r);
        if (!st.ok()) {
            msg = "failed to parse AlterClusterRequest, error: " + st.message().ToString();
            ret = MetaServiceCode::PROTOBUF_PARSE_ERR;
            response_body = msg;
            LOG(WARNING) << msg;
            return;
        }
        r.set_op(AlterClusterRequest::ADD_CLUSTER);
        MetaServiceGenericResponse res;
        alter_cluster(cntl, &r, &res, nullptr);
        ret = res.status().code();
        msg = res.status().msg();
        response_body = msg;
        return;
    }

    if (unresolved_path == "add_node") {
        AlterClusterRequest r;
        auto st = google::protobuf::util::JsonStringToMessage(request_body, &r);
        if (!st.ok()) {
            msg = "failed to parse AlterClusterRequest, error: " + st.message().ToString();
            ret = MetaServiceCode::PROTOBUF_PARSE_ERR;
            response_body = msg;
            LOG(WARNING) << msg;
            return;
        }
        r.set_op(AlterClusterRequest::ADD_NODE);
        MetaServiceGenericResponse res;
        alter_cluster(cntl, &r, &res, nullptr);
        ret = res.status().code();
        msg = res.status().msg();
        response_body = msg;
        return;
    }

    if (unresolved_path == "drop_node") {
        AlterClusterRequest r;
        auto st = google::protobuf::util::JsonStringToMessage(request_body, &r);
        if (!st.ok()) {
            msg = "failed to parse AlterClusterRequest, error: " + st.message().ToString();
            ret = MetaServiceCode::PROTOBUF_PARSE_ERR;
            response_body = msg;
            LOG(WARNING) << msg;
            return;
        }
        r.set_op(AlterClusterRequest::DROP_NODE);
        MetaServiceGenericResponse res;
        alter_cluster(cntl, &r, &res, nullptr);
        ret = res.status().code();
        msg = res.status().msg();
        response_body = msg;
        return;
    }

    // This is useful for debuggin
    if (unresolved_path == "get_cluster") {
        GetClusterRequest r;
        auto st = google::protobuf::util::JsonStringToMessage(request_body, &r);
        if (!st.ok()) {
            msg = "failed to GetClusterRequest, error: " + st.message().ToString();
            ret = MetaServiceCode::PROTOBUF_PARSE_ERR;
            response_body = msg;
            LOG(WARNING) << msg;
            return;
        }
        GetClusterResponse res;
        get_cluster(cntl, &r, &res, nullptr);
        ret = res.status().code();
        msg = res.status().msg();
        response_body = msg;
        return;
    }

    if (unresolved_path == "drop_cluster") {
        AlterClusterRequest r;
        auto st = google::protobuf::util::JsonStringToMessage(request_body, &r);
        if (!st.ok()) {
            msg = "failed to parse AlterClusterRequest, error: " + st.message().ToString();
            ret = MetaServiceCode::PROTOBUF_PARSE_ERR;
            response_body = msg;
            LOG(WARNING) << msg;
            return;
        }
        r.set_op(AlterClusterRequest::DROP_CLUSTER);
        MetaServiceGenericResponse res;
        alter_cluster(cntl, &r, &res, nullptr);
        ret = res.status().code();
        msg = res.status().msg();
        response_body = msg;
        return;
    }

    if (unresolved_path == "rename_cluster") {
        AlterClusterRequest r;
        auto st = google::protobuf::util::JsonStringToMessage(request_body, &r);
        if (!st.ok()) {
            msg = "failed to parse AlterClusterRequest, error: " + st.message().ToString();
            ret = MetaServiceCode::PROTOBUF_PARSE_ERR;
            response_body = msg;
            LOG(WARNING) << msg;
            return;
        }

        r.set_op(AlterClusterRequest::RENAME_CLUSTER);
        MetaServiceGenericResponse res;
        alter_cluster(cntl, &r, &res, nullptr);
        ret = res.status().code();
        msg = res.status().msg();
        response_body = msg;
        return;
    }

    if (unresolved_path == "update_cluster_mysql_user_name") {
        AlterClusterRequest r;
        auto st = google::protobuf::util::JsonStringToMessage(request_body, &r);
        if (!st.ok()) {
            msg = "failed to parse AlterClusterRequest, error: " + st.message().ToString();
            ret = MetaServiceCode::PROTOBUF_PARSE_ERR;
            response_body = msg;
            LOG(WARNING) << msg;
            return;
        }

        r.set_op(AlterClusterRequest::UPDATE_CLUSTER_MYSQL_USER_NAME);
        MetaServiceGenericResponse res;
        alter_cluster(cntl, &r, &res, nullptr);
        ret = res.status().code();
        msg = res.status().msg();
        response_body = msg;
        return;
    }

    if (unresolved_path == "get_tablet_meta") {
        GetTabletStatsRequest r;
        auto st = google::protobuf::util::JsonStringToMessage(request_body, &r);
        if (!st.ok()) {
            msg = "failed to parse GetTabletStatsRequest, error: " + st.message().ToString();
            ret = MetaServiceCode::PROTOBUF_PARSE_ERR;
            response_body = msg;
            LOG(WARNING) << msg;
            return;
        }
        GetTabletStatsResponse res;
        get_tablet_stats(cntl, &r, &res, nullptr);
        ret = res.status().code();
        msg = res.status().msg();
        response_body = msg;
        return;
    }

    if (unresolved_path == "get_stage") {
        GetStageRequest r;
        auto st = google::protobuf::util::JsonStringToMessage(request_body, &r);
        if (!st.ok()) {
            msg = "failed to parse GetStageRequest, error: " + st.message().ToString();
            ret = MetaServiceCode::PROTOBUF_PARSE_ERR;
            response_body = msg;
            LOG(WARNING) << msg;
            return;
        }

        GetStageResponse res;
        get_stage(cntl, &r, &res, nullptr);
        ret = res.status().code();
        msg = res.status().msg();
        response_body = msg;
        return;
    }

    // TODO:
    // * unresolved_path == "encode_key"
    // * unresolved_path == "set_token"
    // * etc.
}

// TODO: move to separate file
//==============================================================================
// Resources
//==============================================================================

void MetaServiceImpl::get_obj_store_info(google::protobuf::RpcController* controller,
                                         const ::selectdb::GetObjStoreInfoRequest* request,
                                         ::selectdb::GetObjStoreInfoResponse* response,
                                         ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request=" << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&ret, &code, &msg, &response, &ctrl](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << (ret == 0 ? "succ to " : "failed to ") << __PRETTY_FUNCTION__ << " "
                          << ctrl->remote_side() << " " << msg;
            });

    // Prepare data
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud unique id not set";
        return;
    }

    std::string instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
        return;
    }

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }
    ret = txn->get(key, &val);
    LOG(INFO) << "get instance_key=" << hex(key);

    std::stringstream ss;
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        ss << "failed to get instance, instance_id=" << instance_id << " ret=" << ret;
        msg = ss.str();
        return;
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse InstanceInfoPB";
        return;
    }

    response->mutable_obj_info()->CopyFrom(instance.obj_info());
    msg = proto_to_json(*response);
    return;
}

void MetaServiceImpl::alter_obj_store_info(google::protobuf::RpcController* controller,
                                           const ::selectdb::AlterObjStoreInfoRequest* request,
                                           ::selectdb::MetaServiceGenericResponse* response,
                                           ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request=" << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&ret, &code, &msg, &response, &ctrl](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << (ret == 0 ? "succ to " : "failed to ") << __PRETTY_FUNCTION__ << " "
                          << ctrl->remote_side() << " " << msg;
            });

    // Prepare data
    if (!request->has_obj() || !request->obj().has_ak() || !request->obj().has_sk()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "s3 obj info err " + proto_to_json(*request);
        return;
    }

    auto& obj = request->obj();
    std::string ak = obj.has_ak() ? obj.ak() : "";
    std::string sk = obj.has_sk() ? obj.sk() : "";
    std::string bucket = obj.has_bucket() ? obj.bucket() : "";
    std::string prefix = obj.has_prefix() ? obj.prefix() : "";
    std::string endpoint = obj.has_endpoint() ? obj.endpoint() : "";
    std::string external_endpoint = obj.has_external_endpoint() ? obj.external_endpoint() : "";
    std::string region = obj.has_region() ? obj.region() : "";

    //  obj size > 1k, refuse
    if (obj.ByteSizeLong() > 1024) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "s3 obj info greater than 1k " + proto_to_json(*request);
        return;
    }

    // TODO(dx): check s3 info right

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud unique id not set";
        return;
    }

    std::string instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
        return;
    }

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }
    ret = txn->get(key, &val);
    LOG(INFO) << "get instance_key=" << hex(key);

    std::stringstream ss;
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        ss << "failed to get instance, instance_id=" << instance_id << " ret=" << ret;
        msg = ss.str();
        return;
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse InstanceInfoPB";
        return;
    }

    if (instance.status() != InstanceInfoPB::NORMAL) {
        code = MetaServiceCode::CLUSTER_NOT_FOUND;
        msg = "instance status has been set delete, plz check it";
        return;
    }

    auto now_time = std::chrono::system_clock::now();
    uint64_t time =
            std::chrono::duration_cast<std::chrono::seconds>(now_time.time_since_epoch()).count();

    switch (request->op()) {
    case AlterObjStoreInfoRequest::UPDATE_AK_SK: {
        // get id
        std::string id = request->obj().has_id() ? request->obj().id() : "0";
        int idx = std::stoi(id);
        if (idx < 1 || idx > instance.obj_info().size()) {
            // err
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "id invalid, please check it";
            return;
        }
        auto& obj_info =
                const_cast<std::decay_t<decltype(instance.obj_info())>&>(instance.obj_info());
        for (auto& it : obj_info) {
            if (std::stoi(it.id()) == idx) {
                if (it.ak() == ak && it.sk() == sk) {
                    // not change, just return ok
                    code = MetaServiceCode::OK;
                    msg = "";
                    return;
                }
                it.set_mtime(time);
                it.set_ak(ak);
                it.set_sk(sk);
            }
        }
    } break;
    case AlterObjStoreInfoRequest::ADD_OBJ_INFO: {
        if (!obj.has_provider()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "s3 conf lease provider info";
            return;
        }
        if (instance.obj_info().size() >= 10) {
            code = MetaServiceCode::UNDEFINED_ERR;
            msg = "this instance history has greater than 10 objs, please new another instance";
            return;
        }
        // ATTN: prefix may be empty
        if (ak.empty() || sk.empty() || bucket.empty() || endpoint.empty() || region.empty()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "s3 conf info err, please check it";
            return;
        }

        auto& objs = instance.obj_info();
        for (auto& it : objs) {
            if (bucket == it.bucket() && prefix == it.prefix() && endpoint == it.endpoint() &&
                region == it.region() && ak == it.ak() && sk == it.sk() &&
                obj.provider() == it.provider() && external_endpoint == it.external_endpoint()) {
                // err, anything not changed
                code = MetaServiceCode::INVALID_ARGUMENT;
                msg = "original obj infos has a same conf, please check it";
                return;
            }
        }
        // calc id
        selectdb::ObjectStoreInfoPB last_item;
        last_item.set_ctime(time);
        last_item.set_mtime(time);
        last_item.set_id(std::to_string(instance.obj_info().size() + 1));
        last_item.set_ak(ak);
        last_item.set_sk(sk);
        last_item.set_bucket(bucket);
        // format prefix, such as `/aa/bb/`, `aa/bb//`, `//aa/bb`, `  /aa/bb` -> `aa/bb`
        prefix = trim(prefix);
        last_item.set_prefix(prefix);
        last_item.set_endpoint(endpoint);
        last_item.set_external_endpoint(external_endpoint);
        last_item.set_region(region);
        last_item.set_provider(obj.provider());
        instance.add_obj_info()->CopyFrom(last_item);
    } break;
    default: {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "invalid request op, op=" << request->op();
        msg = ss.str();
        return;
    }
    }

    LOG(INFO) << "instance " << instance_id << " has " << instance.obj_info().size()
              << " s3 history info, and instance = " << proto_to_json(instance);

    val = instance.SerializeAsString();
    if (val.empty()) {
        msg = "failed to serialize";
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        return;
    }

    txn->put(key, val);
    LOG(INFO) << "put instance_id=" << instance_id << " instance_key=" << hex(key);
    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = "failed to commit kv txn";
        LOG(WARNING) << msg << " ret=" << ret;
    }
    return;
}

void MetaServiceImpl::create_instance(google::protobuf::RpcController* controller,
                                      const ::selectdb::CreateInstanceRequest* request,
                                      ::selectdb::MetaServiceGenericResponse* response,
                                      ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request=" << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&ret, &code, &msg, &response, &ctrl](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << (ret == 0 ? "succ to " : "failed to ") << __PRETTY_FUNCTION__ << " "
                          << ctrl->remote_side() << " " << msg;
            });

    // Prepare data
    auto& obj = request->obj_info();
    std::string ak = obj.has_ak() ? obj.ak() : "";
    std::string sk = obj.has_sk() ? obj.sk() : "";
    std::string bucket = obj.has_bucket() ? obj.bucket() : "";
    std::string prefix = obj.has_prefix() ? obj.prefix() : "";
    // format prefix, such as `/aa/bb/`, `aa/bb//`, `//aa/bb`, `  /aa/bb` -> `aa/bb`
    prefix = trim(prefix);
    std::string endpoint = obj.has_endpoint() ? obj.endpoint() : "";
    std::string external_endpoint = obj.has_external_endpoint() ? obj.external_endpoint() : "";
    std::string region = obj.has_region() ? obj.region() : "";

    // ATTN: prefix may be empty
    if (ak.empty() || sk.empty() || bucket.empty() || endpoint.empty() || region.empty() ||
        !obj.has_provider()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "s3 conf info err, please check it";
        return;
    }

    InstanceInfoPB instance;
    instance.set_instance_id(request->has_instance_id() ? request->instance_id() : "");
    instance.set_user_id(request->has_user_id() ? request->user_id() : "");
    instance.set_name(request->has_name() ? request->name() : "");
    auto obj_info = instance.add_obj_info();
    obj_info->set_ak(ak);
    obj_info->set_sk(sk);
    obj_info->set_bucket(bucket);
    obj_info->set_prefix(prefix);
    obj_info->set_endpoint(endpoint);
    obj_info->set_external_endpoint(external_endpoint);
    obj_info->set_region(region);
    obj_info->set_provider(obj.provider());
    std::ostringstream oss;
    // create instance's s3 conf, id = 1
    obj_info->set_id(std::to_string(1));
    auto now_time = std::chrono::system_clock::now();
    uint64_t time =
            std::chrono::duration_cast<std::chrono::seconds>(now_time.time_since_epoch()).count();
    obj_info->set_ctime(time);
    obj_info->set_mtime(time);

    if (instance.instance_id().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "instance id not set";
        return;
    }

    InstanceKeyInfo key_info {request->instance_id()};
    std::string key;
    std::string val = instance.SerializeAsString();
    instance_key(key_info, &key);
    if (val.empty()) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        msg = "failed to serialize";
        LOG(ERROR) << msg;
        return;
    }

    LOG(INFO) << "xxx instance json=" << proto_to_json(instance);

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }

    // Check existence before proceeding
    ret = txn->get(key, &val);
    if (ret != 1) {
        std::stringstream ss;
        ss << (ret == 0 ? "instance already existed" : "internal error failed to check instance")
           << ", instance_id=" << request->instance_id();
        code = ret == 0 ? MetaServiceCode::ALREADY_EXISTED : MetaServiceCode::UNDEFINED_ERR;
        msg = ss.str();
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }

    txn->put(key, val);
    LOG(INFO) << "put instance_id=" << request->instance_id() << " instance_key=" << hex(key);
    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = "failed to commit kv txn";
        LOG(WARNING) << msg << " ret=" << ret;
    }
}

void MetaServiceImpl::alter_instance(google::protobuf::RpcController* controller,
                                     const ::selectdb::AlterInstanceRequest* request,
                                     ::selectdb::MetaServiceGenericResponse* response,
                                     ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request=" << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    [[maybe_unused]] std::stringstream ss;
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&code, &msg, &response, &ctrl](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << (code == MetaServiceCode::OK ? "succ to " : "failed to ")
                          << __PRETTY_FUNCTION__ << " " << ctrl->remote_side() << " " << msg;
            });

    std::pair<MetaServiceCode, std::string> ret;
    switch (request->op()) {
    case AlterInstanceRequest::DROP: {
        ret = drop_instance(request);
    } break;
    case AlterInstanceRequest::REFRESH: {
        ret = resource_mgr_->refresh_instance(request->instance_id());
    } break;
    default: {
        ss << "invalid request op, op=" << request->op();
        ret = std::make_pair(MetaServiceCode::INVALID_ARGUMENT, ss.str());
    }
    }
    code = ret.first;
    msg = ret.second;

    if (request->op() == AlterInstanceRequest::REFRESH) return;

    auto f = new std::function<void()>([instance_id = request->instance_id(), txn_kv = txn_kv_] {
        notify_refresh_instance(txn_kv, instance_id);
    });
    bthread_t bid;
    if (bthread_start_background(&bid, nullptr, run_bthread_work, f) != 0) {
        LOG(WARNING) << "notify refresh instance inplace, instance_id=" << request->instance_id();
        run_bthread_work(f);
    }
}

std::pair<MetaServiceCode, std::string> MetaServiceImpl::drop_instance(
        const selectdb::AlterInstanceRequest* request) {
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    std::string instance_id = request->has_instance_id() ? request->instance_id() : "";
    if (instance_id.empty()) {
        msg = "instance id not set";
        LOG(WARNING) << msg;
        return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, msg);
    }

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);
    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return std::make_pair(MetaServiceCode::KV_TXN_CREATE_ERR, msg);
    }

    // Check existence before proceeding
    ret = txn->get(key, &val);
    if (ret != 0) {
        std::stringstream ss;
        ss << (ret == 1 ? "instance not existed" : "internal error failed to check instance")
           << ", instance_id=" << request->instance_id();
        // TODO(dx): fix CLUSTER_NOT_FOUND，VERSION_NOT_FOUND，TXN_LABEL_NOT_FOUND，etc to NOT_FOUND
        code = ret == 1 ? MetaServiceCode::CLUSTER_NOT_FOUND : MetaServiceCode::UNDEFINED_ERR;
        msg = ss.str();
        LOG(WARNING) << msg << " ret=" << ret;
        return std::make_pair(code, msg);
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        msg = "failed to parse InstanceInfoPB";
        LOG(WARNING) << msg;
        return std::make_pair(MetaServiceCode::PROTOBUF_PARSE_ERR, msg);
    }

    // check instance doesn't have any cluster.
    if (instance.clusters_size() != 0) {
        msg = "failed to drop instance, instance has clusters";
        LOG(WARNING) << msg;
        return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, msg);
    }

    instance.set_status(InstanceInfoPB::DELETED);

    val = instance.SerializeAsString();
    if (val.empty()) {
        msg = "failed to serialize";
        LOG(ERROR) << msg;
        return std::make_pair(MetaServiceCode::PROTOBUF_SERIALIZE_ERR, msg);
    }
    LOG(INFO) << "put instance_id=" << request->instance_id() << " instance_key=" << hex(key)
              << "drop instance json=" << proto_to_json(instance);

    txn->put(key, val);
    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = "failed to commit kv txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return std::make_pair(code, msg);
    }
    return std::make_pair(code, msg);
}

void MetaServiceImpl::alter_cluster(google::protobuf::RpcController* controller,
                                    const ::selectdb::AlterClusterRequest* request,
                                    ::selectdb::MetaServiceGenericResponse* response,
                                    ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request=" << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg;
    [[maybe_unused]] std::stringstream ss;
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&ret, &code, &msg, &response, &ctrl](int*) {
                msg = msg.empty() ? "OK" : msg;
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << (ret == 0 ? "succ to " : "failed to ") << __PRETTY_FUNCTION__ << " "
                          << ctrl->remote_side() << " " << msg;
            });

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    std::string instance_id = request->has_instance_id() ? request->instance_id() : "";
    if (!cloud_unique_id.empty() && instance_id.empty()) {
        instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
        if (instance_id.empty()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "empty instance_id";
            LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
            return;
        }
    }

    if (instance_id.empty() || !request->has_cluster()) {
        msg = "invalid request instance_id or cluster not given";
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }

    if (!request->has_op()) {
        msg = "op not given";
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }

    LOG(INFO) << "alter cluster instance_id=" << instance_id << " op=" << request->op();
    ClusterInfo cluster;
    cluster.cluster.CopyFrom(request->cluster());

    switch (request->op()) {
    case AlterClusterRequest::ADD_CLUSTER: {
        auto r = resource_mgr_->add_cluster(instance_id, cluster);
        code = r.first;
        msg = r.second;
    } break;
    case AlterClusterRequest::DROP_CLUSTER: {
        auto r = resource_mgr_->drop_cluster(instance_id, cluster);
        code = r.first;
        msg = r.second;
    } break;
    case AlterClusterRequest::UPDATE_CLUSTER_MYSQL_USER_NAME: {
        msg = resource_mgr_->update_cluster(
                instance_id, cluster,
                [&](const ::selectdb::ClusterPB& i) {
                    return i.cluster_id() == cluster.cluster.cluster_id();
                },
                [&](::selectdb::ClusterPB& c, std::set<std::string>& cluster_names) {
                    auto& mysql_user_names = cluster.cluster.mysql_user_name();
                    c.mutable_mysql_user_name()->CopyFrom(mysql_user_names);
                    return "";
                });
    } break;
    case AlterClusterRequest::ADD_NODE: {
        resource_mgr_->check_cluster_params_valid(request->cluster(), &msg, false);
        if (msg != "") {
            LOG(INFO) << msg;
            break;
        }
        std::vector<NodeInfo> to_add;
        std::vector<NodeInfo> to_del;
        for (auto& n : request->cluster().nodes()) {
            NodeInfo node;
            node.instance_id = request->instance_id();
            node.node_info = n;
            node.cluster_id = request->cluster().cluster_id();
            node.cluster_name = request->cluster().cluster_name();
            node.role =
                    (request->cluster().type() == ClusterPB::SQL
                             ? Role::SQL_SERVER
                             : (request->cluster().type() == ClusterPB::COMPUTE ? Role::COMPUTE_NODE
                                                                                : Role::UNDEFINED));
            to_add.emplace_back(std::move(node));
        }
        msg = resource_mgr_->modify_nodes(instance_id, to_add, to_del);
    } break;
    case AlterClusterRequest::DROP_NODE: {
        resource_mgr_->check_cluster_params_valid(request->cluster(), &msg, false);
        if (msg != "") {
            LOG(INFO) << msg;
            break;
        }
        std::vector<NodeInfo> to_add;
        std::vector<NodeInfo> to_del;
        for (auto& n : request->cluster().nodes()) {
            NodeInfo node;
            node.instance_id = request->instance_id();
            node.node_info = n;
            node.cluster_id = request->cluster().cluster_id();
            node.cluster_name = request->cluster().cluster_name();
            node.role =
                    (request->cluster().type() == ClusterPB::SQL
                             ? Role::SQL_SERVER
                             : (request->cluster().type() == ClusterPB::COMPUTE ? Role::COMPUTE_NODE
                                                                                : Role::UNDEFINED));
            to_del.emplace_back(std::move(node));
        }
        msg = resource_mgr_->modify_nodes(instance_id, to_add, to_del);
    } break;
    case AlterClusterRequest::RENAME_CLUSTER: {
        msg = resource_mgr_->update_cluster(
                instance_id, cluster,
                [&](const ::selectdb::ClusterPB& i) {
                    return i.cluster_id() == cluster.cluster.cluster_id();
                },
                [&](::selectdb::ClusterPB& c, std::set<std::string>& cluster_names) {
                    std::string msg = "";
                    auto it = cluster_names.find(cluster.cluster.cluster_name());
                    LOG(INFO) << "cluster.cluster.cluster_name(): "
                              << cluster.cluster.cluster_name();
                    for (auto itt : cluster_names) {
                        LOG(INFO) << "itt : " << itt;
                    }
                    if (it != cluster_names.end()) {
                        code = MetaServiceCode::INVALID_ARGUMENT;
                        ss << "failed to rename cluster, a cluster with the same name already "
                              "exists in this instance "
                           << proto_to_json(c);
                        msg = ss.str();
                        return msg;
                    }
                    if (c.cluster_name() == cluster.cluster.cluster_name()) {
                        code = MetaServiceCode::INVALID_ARGUMENT;
                        ss << "failed to rename cluster, name eq original name, original cluster "
                              "is "
                           << proto_to_json(c);
                        msg = ss.str();
                        return msg;
                    }
                    c.set_cluster_name(cluster.cluster.cluster_name());
                    return msg;
                });
    } break;
    default: {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "invalid request op, op=" << request->op();
        msg = ss.str();
        return;
    }
    }
    if (!msg.empty() && code == MetaServiceCode::OK) {
        code = MetaServiceCode::UNDEFINED_ERR;
    }

    if (code != MetaServiceCode::OK) return;

    auto f = new std::function<void()>([instance_id = request->instance_id(), txn_kv = txn_kv_] {
        notify_refresh_instance(txn_kv, instance_id);
    });
    bthread_t bid;
    if (bthread_start_background(&bid, nullptr, run_bthread_work, f) != 0) {
        LOG(WARNING) << "notify refresh instance inplace, instance_id=" << request->instance_id();
        run_bthread_work(f);
    }
} // alter cluster

void MetaServiceImpl::get_cluster(google::protobuf::RpcController* controller,
                                  const ::selectdb::GetClusterRequest* request,
                                  ::selectdb::GetClusterResponse* response,
                                  ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request=" << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    [[maybe_unused]] std::stringstream ss;
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&code, &msg, &response, &ctrl](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << (code == MetaServiceCode::OK ? "succ to " : "failed to ")
                          << __PRETTY_FUNCTION__ << " " << ctrl->remote_side() << " " << msg;
            });

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    std::string cluster_id = request->has_cluster_id() ? request->cluster_id() : "";
    std::string cluster_name = request->has_cluster_name() ? request->cluster_name() : "";
    std::string mysql_user_name = request->has_mysql_user_name() ? request->mysql_user_name() : "";

    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud_unique_id must be given";
        return;
    }

    std::string instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        if (request->has_instance_id()) {
            instance_id = request->instance_id();
            // FIXME(gavin): this mechanism benifits debugging and
            //               administration, is it dangerous?
            LOG(WARNING) << "failed to get instance_id with cloud_unique_id=" << cloud_unique_id
                         << " use the given instance_id=" << instance_id << " instead";
        } else {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "empty instance_id";
            LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
            return;
        }
    }

    // ATTN: if the case that multiple conditions are satisfied, just use by this order:
    // cluster_id -> cluster_name -> mysql_user_name
    if (!cluster_id.empty()) {
        cluster_name = "";
        mysql_user_name = "";
    } else if (!cluster_name.empty()) {
        mysql_user_name = "";
    }

    bool get_all_cluster_info = false;
    // if cluster_id、cluster_name、mysql_user_name all empty, get this instance's all cluster info.
    if (cluster_id.empty() && cluster_name.empty() && mysql_user_name.empty()) {
        get_all_cluster_info = true;
    }

    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "failed to get instance_id with cloud_unique_id=" + cloud_unique_id;
        return;
    }

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    int ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }
    ret = txn->get(key, &val);
    LOG(INFO) << "get instance_key=" << hex(key);

    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        ss << "failed to get instance, instance_id=" << instance_id << " ret=" << ret;
        msg = ss.str();
        return;
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse InstanceInfoPB";
        return;
    }

    auto get_cluster_mysql_user = [](const ::selectdb::ClusterPB& c,
                                     std::set<std::string>* mysql_users) {
        for (int i = 0; i < c.mysql_user_name_size(); i++) {
            mysql_users->emplace(c.mysql_user_name(i));
        }
    };

    if (get_all_cluster_info) {
        response->mutable_cluster()->CopyFrom(instance.clusters());
        msg = proto_to_json(*response);
        LOG(INFO) << "get all cluster info, " << msg;
    } else {
        for (int i = 0; i < instance.clusters_size(); ++i) {
            auto& c = instance.clusters(i);
            std::set<std::string> mysql_users;
            get_cluster_mysql_user(c, &mysql_users);
            // The last wins if add_cluster() does not ensure uniqueness of
            // cluster_id and cluster_name respectively
            if ((c.has_cluster_name() && c.cluster_name() == cluster_name) ||
                (c.has_cluster_id() && c.cluster_id() == cluster_id) ||
                mysql_users.count(mysql_user_name)) {
                // just one cluster
                response->add_cluster()->CopyFrom(c);
                msg = proto_to_json(response->cluster().at(0));
                LOG(INFO) << "found a cluster, instance_id=" << instance.instance_id()
                          << " cluster=" << msg;
            }
        }
    }

    if (response->cluster().size() == 0) {
        ss << "fail to get cluster with " << request->DebugString();
        msg = ss.str();
        std::replace(msg.begin(), msg.end(), '\n', ' ');
        code = MetaServiceCode::CLUSTER_NOT_FOUND;
        return;
    }

} // get_cluster

void MetaServiceImpl::create_stage(::google::protobuf::RpcController* controller,
                                   const ::selectdb::CreateStageRequest* request,
                                   ::selectdb::CreateStageResponse* response,
                                   ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request=" << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&ret, &code, &msg, &response, &ctrl](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << (ret == 0 ? "succ to " : "failed to ") << __PRETTY_FUNCTION__ << " "
                          << ctrl->remote_side() << " " << msg;
            });

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud unique id not set";
        return;
    }

    std::string instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
        return;
    }

    if (!request->has_stage()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "stage not set";
        return;
    }
    auto stage = request->stage();

    if (!stage.has_type()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "stage type not set";
        return;
    }
    if (stage.type() == StagePB::INTERNAL) {
        code = MetaServiceCode::UNDEFINED_ERR;
        msg = "create internal stage is unsupported";
        return;
    }
    if (stage.name().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "stage name not set";
        return;
    }
    if (stage.stage_id().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "stage id not set";
        return;
    }

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }
    ret = txn->get(key, &val);
    LOG(INFO) << "get instance_key=" << hex(key);

    std::stringstream ss;
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        ss << "failed to get instance, instance_id=" << instance_id << " ret=" << ret;
        msg = ss.str();
        return;
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse InstanceInfoPB";
        return;
    }

    if (instance.stages_size() >= 20) {
        code = MetaServiceCode::UNDEFINED_ERR;
        msg = "this instance has greater than 20 stages";
        return;
    }

    // check if the stage exists
    for (int i = 0; i < instance.stages_size(); ++i) {
        auto& s = instance.stages(i);
        if (s.type() == stage.type() && s.name() == stage.name()) {
            code = MetaServiceCode::ALREADY_EXISTED;
            msg = "stage already exist";
            return;
        }
        if (s.stage_id() == stage.stage_id()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "stage id is duplicated";
            return;
        }
    }

    instance.add_stages()->CopyFrom(stage);
    val = instance.SerializeAsString();
    if (val.empty()) {
        msg = "failed to serialize";
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        return;
    }

    txn->put(key, val);
    LOG(INFO) << "put instance_id=" << instance_id << " instance_key=" << hex(key);
    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = "failed to commit kv txn";
        LOG(WARNING) << msg << " ret=" << ret;
    }
}

void MetaServiceImpl::get_stage(google::protobuf::RpcController* controller,
                                const ::selectdb::GetStageRequest* request,
                                ::selectdb::GetStageResponse* response,
                                ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request=" << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&ret, &code, &msg, &response, &ctrl](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << (ret == 0 ? "succ to " : "failed to ") << __PRETTY_FUNCTION__ << " "
                          << ctrl->remote_side() << " " << msg;
            });

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud unique id not set";
        return;
    }

    std::string instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
        return;
    }

    if (!request->has_type()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "stage type not set";
        return;
    }
    auto type = request->type();

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }
    ret = txn->get(key, &val);
    LOG(INFO) << "get instance_key=" << hex(key);

    std::stringstream ss;
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        ss << "failed to get instance, instance_id=" << instance_id << " ret=" << ret;
        msg = ss.str();
        return;
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse InstanceInfoPB";
        return;
    }

    if (type == StagePB::INTERNAL) {
        auto mysql_user_name = request->has_mysql_user_name() ? request->mysql_user_name() : "";
        if (mysql_user_name.empty()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "mysql user name not set";
            return;
        }

        // check mysql user_name has been created internal stage
        auto& stage = instance.stages();
        bool found = false;
        if (instance.obj_info_size() == 0) {
            LOG(WARNING) << "impossible, instance must have at least one obj_info.";
            code = MetaServiceCode::UNDEFINED_ERR;
            msg = "impossible, instance must have at least one obj_info.";
            return;
        }
        auto& lastest_obj = instance.obj_info()[instance.obj_info_size() - 1];

        for (auto s : stage) {
            if (s.type() != StagePB::INTERNAL || s.mysql_user_name().size() == 0) {
                continue;
            }
            if (s.mysql_user_name()[0] == mysql_user_name) {
                // find, use it stage prefix and id
                found = true;
                // get from internal stage
                int idx = stoi(s.obj_info().id());
                if (idx > 10 || idx < 1) {
                    LOG(WARNING) << "invalid idx: " << idx;
                    code = MetaServiceCode::UNDEFINED_ERR;
                    msg = "impossible, id invalid";
                    return;
                }
                auto& old_obj = instance.obj_info()[idx - 1];

                StagePB stage_pb;
                stage_pb.mutable_obj_info()->set_ak(old_obj.ak());
                stage_pb.mutable_obj_info()->set_sk(old_obj.sk());
                stage_pb.mutable_obj_info()->set_bucket(old_obj.bucket());
                stage_pb.mutable_obj_info()->set_endpoint(old_obj.endpoint());
                stage_pb.mutable_obj_info()->set_external_endpoint(old_obj.external_endpoint());
                stage_pb.mutable_obj_info()->set_region(old_obj.region());
                stage_pb.mutable_obj_info()->set_provider(old_obj.provider());
                stage_pb.mutable_obj_info()->set_prefix(s.obj_info().prefix());
                stage_pb.set_stage_id(s.stage_id());
                stage_pb.set_type(s.type());
                msg = proto_to_json(stage_pb);
                response->add_stage()->CopyFrom(stage_pb);
                return;
            }
        }
        // if not, create it
        if (!found) {
            if (stage.size() >= 20) {
                LOG(WARNING) << "can't create more than 20 stages, and instance has "
                             << std::to_string(stage.size());
                msg = "can't create more than 20 stages";
                code = MetaServiceCode::ALREADY_EXISTED;
                return;
            }
            // ${prefix}/stage/user/username
            std::string prefix =
                    fmt::format("{}/stage/user/{}/", lastest_obj.prefix(), mysql_user_name);
            auto as = instance.add_stages();
            as->mutable_obj_info()->set_prefix(prefix);
            as->mutable_obj_info()->set_id(lastest_obj.id());
            as->add_mysql_user_name(mysql_user_name);
            // ATTN: external stage_id is uuid, set in fe. internal stage_id set in be.
            // stage_id need to be unique, But I don't want to introduce the calculation method of uuid
            std::string stage_id = fmt::format("{}_{}", lastest_obj.id(), mysql_user_name);
            as->set_stage_id(stage_id);

            txn->put(key, instance.SerializeAsString());
            LOG(INFO) << "put instance_id=" << instance_id << " instance_key=" << hex(key)
                      << " json=" << proto_to_json(instance);
            ret = txn->commit();
            if (ret != 0) {
                code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT
                                 : MetaServiceCode::KV_TXN_COMMIT_ERR;
                msg = "failed to commit kv txn";
                LOG(WARNING) << msg << " ret=" << ret;
                return;
            }
            StagePB stage_pb;
            stage_pb.mutable_obj_info()->set_prefix(prefix);
            stage_pb.mutable_obj_info()->set_ak(lastest_obj.ak());
            stage_pb.mutable_obj_info()->set_sk(lastest_obj.sk());
            stage_pb.mutable_obj_info()->set_bucket(lastest_obj.bucket());
            stage_pb.mutable_obj_info()->set_endpoint(lastest_obj.endpoint());
            stage_pb.mutable_obj_info()->set_external_endpoint(lastest_obj.external_endpoint());
            stage_pb.mutable_obj_info()->set_region(lastest_obj.region());
            stage_pb.mutable_obj_info()->set_provider(lastest_obj.provider());
            stage_pb.set_stage_id(stage_id);
            stage_pb.set_type(StagePB::INTERNAL);
            msg = proto_to_json(stage_pb);
            response->add_stage()->CopyFrom(stage_pb);
            return;
        }
    }

    // get all external stages
    if (type == StagePB::EXTERNAL && !request->has_stage_name()) {
        for (int i = 0; i < instance.stages_size(); ++i) {
            auto& s = instance.stages(i);
            if (s.type() != StagePB::EXTERNAL) {
                continue;
            }
            response->add_stage()->CopyFrom(s);
        }
        return;
    }

    // get external stage with the specified stage name
    for (int i = 0; i < instance.stages_size(); ++i) {
        auto& s = instance.stages(i);
        if (s.type() == type && s.name() == request->stage_name()) {
            response->add_stage()->CopyFrom(s);
            return;
        }
    }

    ss << "stage not found with " << proto_to_json(*request);
    msg = ss.str();
    code = MetaServiceCode::STAGE_NOT_FOUND;
}

void MetaServiceImpl::begin_copy(google::protobuf::RpcController* controller,
                                 const ::selectdb::BeginCopyRequest* request,
                                 ::selectdb::BeginCopyResponse* response,
                                 ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request=" << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&ret, &code, &msg, &response, &ctrl](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << (ret == 0 ? "succ to " : "failed to ") << __PRETTY_FUNCTION__ << " "
                          << ctrl->remote_side() << " " << msg;
            });

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud unique id not set";
        return;
    }

    std::string instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
        return;
    }

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }

    // scan loading or loaded files in copy jobs
    auto object_files = request->object_files();
    std::set<int> remove_index;
    CopyJobKeyInfo key_info0 {instance_id, request->stage_id(), request->table_id(), "", 0};
    CopyJobKeyInfo key_info1 {instance_id, request->stage_id(), request->table_id() + 1, "", 0};
    std::string key0;
    std::string key1;
    copy_job_key(key_info0, &key0);
    copy_job_key(key_info1, &key1);
    std::unique_ptr<RangeGetIterator> it;
    do {
        ret = txn->get(key0, key1, &it);
        if (ret != 0) {
            code = MetaServiceCode::KV_TXN_GET_ERR;
            msg = "failed to get copy jobs";
            LOG(WARNING) << msg << " ret=" << ret;
            return;
        }

        while (it->has_next()) {
            auto [k, v] = it->next();
            if (!it->has_next()) key0 = k;
            CopyJobPB copy_job;
            if (!copy_job.ParseFromArray(v.data(), v.size())) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = "failed to parse CopyJobPB";
                return;
            }
            // TODO check if job is timeout
            for (const auto& file : copy_job.object_files()) {
                for (auto i = 0; i < object_files.size(); ++i) {
                    if (!remove_index.count(i) &&
                        object_files.at(i).relative_path() == file.relative_path() &&
                        object_files.at(i).etag() == file.etag()) {
                        remove_index.insert(i);
                    }
                }
            }
        }
        key0.push_back('\x00');
    } while (it->more());

    // copy job key
    CopyJobKeyInfo key_info {instance_id, request->stage_id(), request->table_id(),
                             request->copy_id(), request->group_id()};
    std::string key;
    std::string val;
    copy_job_key(key_info, &key);
    // copy job value
    CopyJobPB copy_job;
    copy_job.set_stage_type(request->stage_type());
    copy_job.set_job_status(CopyJobPB::LOADING);
    copy_job.set_start_time(request->start_time());
    copy_job.set_timeout_time(request->timeout_time());

    std::vector<std::pair<std::string, std::string>> copy_files;
    for (auto i = 0; i < object_files.size(); ++i) {
        if (remove_index.count(i)) {
            continue;
        }
        auto& file = object_files.at(i);
        copy_job.add_object_files()->CopyFrom(file);
        // copy file key
        CopyFileKeyInfo file_key_info {instance_id, request->stage_id(), request->table_id(),
                                       file.relative_path(), file.etag()};
        std::string file_key;
        copy_file_key(file_key_info, &file_key);
        // copy file value
        CopyFilePB copy_file;
        copy_file.set_copy_id(request->copy_id());
        copy_file.set_group_id(request->group_id());
        std::string file_val = copy_file.SerializeAsString();
        if (file_val.empty()) {
            msg = "failed to serialize";
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            return;
        }
        copy_files.emplace_back(std::move(file_key), std::move(file_val));
        response->add_filtered_object_files()->CopyFrom(file);
    }

    val = copy_job.SerializeAsString();
    if (val.empty()) {
        msg = "failed to serialize";
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        return;
    }
    // put copy job
    txn->put(key, val);
    LOG(INFO) << "put copy_job_key=" << hex(key);
    // put copy file
    for (const auto& [k, v] : copy_files) {
        txn->put(k, v);
        LOG(INFO) << "put copy_file_key=" << hex(k);
    }

    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = "failed to commit kv txn";
        LOG(WARNING) << msg << " ret=" << ret;
    }
}

void MetaServiceImpl::finish_copy(google::protobuf::RpcController* controller,
                                  const ::selectdb::FinishCopyRequest* request,
                                  ::selectdb::FinishCopyResponse* response,
                                  ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request=" << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&ret, &code, &msg, &response, &ctrl](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << (ret == 0 ? "succ to " : "failed to ") << __PRETTY_FUNCTION__ << " "
                          << ctrl->remote_side() << " " << msg;
            });

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud unique id not set";
        return;
    }

    std::string instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
        return;
    }

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }

    // copy job key
    CopyJobKeyInfo key_info {instance_id, request->stage_id(), request->table_id(),
                             request->copy_id(), request->group_id()};
    std::string key;
    std::string val;
    copy_job_key(key_info, &key);
    ret = txn->get(key, &val);

    std::stringstream ss;
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        ss << "failed to get instance, instance_id=" << instance_id << " ret=" << ret;
        msg = ss.str();
        return;
    }

    CopyJobPB copy_job;
    if (!copy_job.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse CopyJobPB";
        return;
    }

    std::vector<std::string> copy_files;
    for (const auto& file : copy_job.object_files()) {
        // copy file key
        CopyFileKeyInfo file_key_info {instance_id, request->stage_id(), request->table_id(),
                                       file.relative_path(), file.etag()};
        std::string file_key;
        copy_file_key(file_key_info, &file_key);
        copy_files.emplace_back(std::move(file_key));
    }

    if (request->action() == FinishCopyRequest::COMMIT) {
        // update copy job status from Loading to Finish
        copy_job.set_job_status(CopyJobPB::FINISH);
        val = copy_job.SerializeAsString();
        if (val.empty()) {
            msg = "failed to serialize";
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            return;
        }
        txn->put(key, val);
        LOG(INFO) << "put finished copy_job_key=" << hex(key);
    } else if (request->action() == FinishCopyRequest::ABORT) {
        // remove copy job
        txn->remove(key);
        LOG(INFO) << "remove aborted copy_job_key=" << hex(key);
    } else {
        msg = "Unhandled action";
        code = MetaServiceCode::UNDEFINED_ERR;
        return;
    }
    // remove copy file
    for (const auto& k : copy_files) {
        txn->remove(k);
        LOG(INFO) << "remove copy_file_key=" << hex(k);
    }

    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = "failed to commit kv txn";
        LOG(WARNING) << msg << " ret=" << ret;
    }
}

void MetaServiceImpl::get_copy_files(google::protobuf::RpcController* controller,
                                     const ::selectdb::GetCopyFilesRequest* request,
                                     ::selectdb::GetCopyFilesResponse* response,
                                     ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request=" << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&ret, &code, &msg, &response, &ctrl](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << (ret == 0 ? "succ to " : "failed to ") << __PRETTY_FUNCTION__ << " "
                          << ctrl->remote_side() << " " << msg;
            });

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud unique id not set";
        return;
    }

    std::string instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
        return;
    }

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }

    CopyJobKeyInfo key_info0 {instance_id, request->stage_id(), request->table_id(), "", 0};
    CopyJobKeyInfo key_info1 {instance_id, request->stage_id(), request->table_id() + 1, "", 0};
    std::string key0;
    std::string key1;
    copy_job_key(key_info0, &key0);
    copy_job_key(key_info1, &key1);
    std::unique_ptr<RangeGetIterator> it;
    do {
        ret = txn->get(key0, key1, &it);
        if (ret != 0) {
            code = MetaServiceCode::KV_TXN_GET_ERR;
            msg = "failed to get copy jobs";
            LOG(WARNING) << msg << " ret=" << ret;
            return;
        }

        while (it->has_next()) {
            auto [k, v] = it->next();
            if (!it->has_next()) key0 = k;
            CopyJobPB copy_job;
            if (!copy_job.ParseFromArray(v.data(), v.size())) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = "failed to parse CopyJobPB";
                return;
            }
            // TODO check if job is timeout
            for (const auto& file : copy_job.object_files()) {
                response->add_object_files()->CopyFrom(file);
            }
        }
        key0.push_back('\x00');
    } while (it->more());
}

void notify_refresh_instance(std::shared_ptr<TxnKv> txn_kv, const std::string& instance_id) {
    LOG(INFO) << "begin notify_refresh_instance";
    std::unique_ptr<Transaction> txn;
    int ret = txn_kv->create_txn(&txn);
    if (ret != 0) {
        LOG(WARNING) << "failed to create txn"
                     << " ret=" << ret;
        return;
    }
    std::string key = system_meta_service_registry_key();
    std::string val;
    ret = txn->get(key, &val);
    if (ret != 0) {
        LOG(WARNING) << "failed to get server registry"
                     << " ret=" << ret;
        return;
    }
    std::string self_endpoint =
            std::string(butil::my_ip_cstr()) + ":" + std::to_string(config::brpc_listen_port);
    ServiceRegistryPB reg;
    reg.ParseFromString(val);
    brpc::ChannelOptions options;
    static std::unordered_map<std::string, std::shared_ptr<MetaService_Stub>> stubs;
    static std::mutex mtx;
    std::vector<bthread_t> btids;
    btids.reserve(reg.items_size());
    for (int i = 0; i < reg.items_size(); ++i) {
        ret = 0;
        auto& e = reg.items(i);
        auto endpoint = e.ip() + ":" + std::to_string(e.port());
        if (endpoint == self_endpoint) continue;

        // Prepare stub
        std::shared_ptr<MetaService_Stub> stub;
        do {
            std::lock_guard l(mtx);
            if (auto it = stubs.find(endpoint); it != stubs.end()) {
                stub = it->second;
                break;
            }
            auto channel = std::make_unique<brpc::Channel>();
            ret = channel->Init(endpoint.c_str(), &options);
            if (ret != 0) {
                LOG(WARNING) << "fail to init brpc channel, endpoint=" << endpoint;
                break;
            }
            stub = std::make_shared<MetaService_Stub>(channel.release(),
                                                      google::protobuf::Service::STUB_OWNS_CHANNEL);
        } while (false);
        if (ret != 0) continue;

        // Issue RPC
        auto f = new std::function<void()>([instance_id, stub, endpoint] {
            int num_try = 0;
            bool succ = false;
            while (num_try++ < 3) {
                brpc::Controller cntl;
                cntl.set_timeout_ms(3000);
                AlterInstanceRequest req;
                MetaServiceGenericResponse res;
                req.set_instance_id(instance_id);
                req.set_op(AlterInstanceRequest::REFRESH);
                stub->alter_instance(&cntl, &req, &res, nullptr);
                succ = res.status().code() == MetaServiceCode::OK;
                LOG(INFO) << (succ ? "succ" : "failed")
                          << " to issue refresh_instance rpc, num_try=" << num_try
                          << " endpoint=" << endpoint << " response=" << proto_to_json(res);
                if (succ) return;
                bthread_usleep(300000);
            }
            if (succ) return;
            LOG(WARNING) << "failed to refresh finally, it may left the system inconsistent,"
                         << " tired=" << num_try;
        });
        bthread_t bid;
        ret = bthread_start_background(&bid, nullptr, run_bthread_work, f);
        if (ret != 0) continue;
        btids.emplace_back(bid);
    } // for
    for (auto& i : btids) bthread_join(i, nullptr);
    LOG(INFO) << "finish notify_refresh_instance, num_items=" << reg.items_size();
}

} // namespace selectdb
// vim: et ts=4 sw=4 cc=80:
