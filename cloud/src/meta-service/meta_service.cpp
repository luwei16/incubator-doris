
// clang-format off
#include "meta_service.h"
#include "meta-service/keys.h"

#include "brpc/closure_guard.h"
#include "brpc/controller.h"

#include <limits>
#include <memory>
// clang-format on

// FIXME: remove duplicated code
#include <iomanip>
#include <sstream>
static std::string hex(std::string_view str) {
    std::stringstream ss;
    for (auto& i : str) {
        ss << std::hex << std::setw(2) << std::setfill('0') << ((int16_t)i & 0xff);
    }
    return ss.str();
}
namespace selectdb {

MetaServiceImpl::MetaServiceImpl(std::shared_ptr<TxnKv> txn_kv) {
    txn_kv_ = txn_kv;
}

MetaServiceImpl::~MetaServiceImpl() {}

void MetaServiceImpl::begin_txn(::google::protobuf::RpcController* controller,
                                const ::selectdb::BeginTxnRequest* request,
                                ::selectdb::BeginTxnResponse* response,
                                ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request: " << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    std::string msg = "OK";
    std::unique_ptr<int, std::function<void(int*)>> defer_status((int*)0x01, [&ret, &msg, &response,
                                                                              &ctrl](int*) {
        response->mutable_status()->set_code(ret);
        response->mutable_status()->set_msg(msg);
        LOG(INFO) << "finish " << __PRETTY_FUNCTION__ << " " << ctrl->remote_side() << " " << msg;
    });
}

void MetaServiceImpl::precommit_txn(::google::protobuf::RpcController* controller,
                                    const ::selectdb::PrecommitTxnRequest* request,
                                    ::selectdb::PrecommitTxnResponse* response,
                                    ::google::protobuf::Closure* done) {}

void MetaServiceImpl::commit_txn(::google::protobuf::RpcController* controller,
                                 const ::selectdb::CommitTxnRequest* request,
                                 ::selectdb::CommitTxnResponse* response,
                                 ::google::protobuf::Closure* done) {}

void MetaServiceImpl::get_version(::google::protobuf::RpcController* controller,
                                  const ::selectdb::GetVersionRequest* request,
                                  ::selectdb::GetVersionResponse* response,
                                  ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request: " << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    std::unique_ptr<Transaction> txn;
    int ret = 0;
    std::string msg = "OK";
    std::unique_ptr<int, std::function<void(int*)>> defer_status((int*)0x01, [&ret, &msg, &response,
                                                                              &ctrl](int*) {
        response->mutable_status()->set_code(ret);
        response->mutable_status()->set_msg(msg);
        LOG(INFO) << "rpc from " << ctrl->remote_side() << " response: " << response->DebugString();
    });

    // TODO(dx): For auth
    std::string cloud_unique_id = "";
    if (request->has_cloud_unique_id()) {
        cloud_unique_id = request->cloud_unique_id();
    }

    int64_t db_id = request->has_db_id() ? request->db_id() : -1;
    int64_t table_id = request->has_table_id() ? request->table_id() : -1;
    int64_t partition_id = request->has_partition_id() ? request->partition_id() : -1;
    if (db_id == -1 || table_id == -1 || partition_id == -1) {
        msg = "params error, db_id: " + std::to_string(db_id) +
              " table_id: " + std::to_string(table_id) +
              " partition_id: " + std::to_string(partition_id);
        ret = -1;
        LOG(WARNING) << msg;
        return;
    }

    // TODO(dx): fix it, use instance_id later.
    VersionKeyInfo v_key {"instance_id_deadbeef", db_id, table_id, partition_id};
    std::string encoded_version_key;
    version_key(v_key, &encoded_version_key);

    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        msg = "failed to create txn";
        ret = -1;
        return;
    }

    std::string val;
    // 0 for success get a key, 1 for key not found, negative for error
    ret = txn->get(encoded_version_key, &val);
    if (ret == 0) {
        int64_t version = *reinterpret_cast<int64_t*>(val.data());
        response->set_version(version);
        return;
    } else if (ret == 1) {
        msg = "not found";
        // TODO(dx): find error code enum in proto, or add
        ret = -2;
        return;
    }
    msg = "failed to get txn";
    ret = -1;
}

void MetaServiceImpl::create_tablet(::google::protobuf::RpcController* controller,
                                    const ::selectdb::CreateTabletRequest* request,
                                    ::selectdb::MetaServiceGenericResponse* response,
                                    ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request: " << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    std::string msg = "OK";
    std::unique_ptr<int, std::function<void(int*)>> defer_status((int*)0x01, [&ret, &msg, &response,
                                                                              &ctrl](int*) {
        response->mutable_status()->set_code(ret);
        response->mutable_status()->set_msg(msg);
        LOG(INFO) << "finish " << __PRETTY_FUNCTION__ << " " << ctrl->remote_side() << " " << msg;
    });

    if (!request->has_tablet_meta()) {
        msg = "no tablet meta";
        ret = -1;
        return;
    }

    // TODO: validate tablet meta, check existence
    int64_t table_id = request->tablet_meta().table_id();
    int64_t tablet_id = request->tablet_meta().tablet_id();

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);

    MetaTabletKeyInfo key_info {"instance_id_deadbeef", table_id, tablet_id};
    std::string key;
    std::string val;
    meta_tablet_key(key_info, &key);
    if (!request->tablet_meta().SerializeToString(&val)) {
        ret = -2;
        msg = "failed to serialize tablet meta";
        return;
    }
    txn->put(key, val);

    LOG(INFO) << "xxx tablet key: " << hex(key);

    // Index tablet_id -> table_id
    std::string key1;
    std::string val1(reinterpret_cast<char*>(&table_id), sizeof(table_id));
    MetaTabletTblKeyInfo key_info1 {"instance_id_deadbeef", tablet_id};
    meta_tablet_table_key(key_info1, &key1);
    txn->put(key1, val1);

    LOG(INFO) << "xxx tablet -> table key: " << hex(key);

    ret = txn->commit();
    if (ret != 0) {
        ret = -3;
        msg = "failed to save tablet meta";
        return;
    }
}

void MetaServiceImpl::drop_tablet(::google::protobuf::RpcController* controller,
                                  const ::selectdb::DropTabletRequest* request,
                                  ::selectdb::MetaServiceGenericResponse* response,
                                  ::google::protobuf::Closure* done) {}

void MetaServiceImpl::get_tablet(::google::protobuf::RpcController* controller,
                                 const ::selectdb::GetTabletRequest* request,
                                 ::selectdb::GetTabletResponse* response,
                                 ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request: " << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    std::string msg = "OK";
    std::unique_ptr<int, std::function<void(int*)>> defer_status((int*)0x01, [&ret, &msg, &response,
                                                                              &ctrl](int*) {
        response->mutable_status()->set_code(ret);
        response->mutable_status()->set_msg(msg);
        LOG(INFO) << "finish " << __PRETTY_FUNCTION__ << " " << ctrl->remote_side() << " " << msg;
    });

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);

    // TODO: validate request
    int64_t tablet_id = request->tablet_id();
    MetaTabletTblKeyInfo key_info0 {"instance_id_deadbeef", tablet_id};
    std::string key0;
    std::string val0;
    meta_tablet_table_key(key_info0, &key0);
    ret = txn->get(key0, &val0);
    if (ret != 0) {
        msg = "failed to get table id from tablet_id";
        return;
    }

    int64_t table_id = *reinterpret_cast<int64_t*>(val0.data());
    MetaTabletKeyInfo key_info1 {"instance_id_deadbeef", table_id, tablet_id};
    std::string key1;
    std::string val1;
    meta_tablet_key(key_info1, &key1);
    ret = txn->get(key1, &val1);
    if (ret != 0) {
        msg = "failed to get tablet";
        msg += (ret == 1 ? ": not found" : "");
        return;
    }

    if (!response->mutable_tablet_meta()->ParseFromString(val1)) {
        ret = -3;
        msg = "malformed tablet meta, unable to initialize";
        return;
    }
}

void MetaServiceImpl::create_rowset(::google::protobuf::RpcController* controller,
                                    const ::selectdb::CreateRowsetRequest* request,
                                    ::selectdb::MetaServiceGenericResponse* response,
                                    ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request: " << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    std::string msg = "OK";
    std::unique_ptr<int, std::function<void(int*)>> defer_status((int*)0x01, [&ret, &msg, &response,
                                                                              &ctrl](int*) {
        response->mutable_status()->set_code(ret);
        response->mutable_status()->set_msg(msg);
        LOG(INFO) << "finish " << __PRETTY_FUNCTION__ << " " << ctrl->remote_side() << " " << msg;
    });

    if (!request->has_rowset_meta()) {
        ret = -1;
        msg = "no rowset meta";
        return;
    }
    // TODO: validate rowset meta, check existence
    bool temporary = request->has_temporary() && request->temporary() ? true : false;
    int64_t tablet_id = request->rowset_meta().tablet_id();
    int64_t end_version = request->rowset_meta().end_version();
    std::string rowset_id = request->rowset_meta().rowset_id_v2();

    std::string key;
    std::string val;

    if (temporary) {
        int64_t txn_id = request->rowset_meta().txn_id();
        MetaRowsetTmpKeyInfo key_info {"instance_id_deadbeef", txn_id, rowset_id};
        meta_rowset_tmp_key(key_info, &key);
    } else {
        MetaRowsetKeyInfo key_info {"instance_id_deadbeef", tablet_id, end_version, rowset_id};
        meta_rowset_key(key_info, &key);
    }

    if (!request->rowset_meta().SerializeToString(&val)) {
        ret = -2;
        msg = "failed to serialize rowset meta";
        return;
    }

    LOG(INFO) << "xxx create_rowset key " << hex(key);

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    txn->put(key, val);
    ret = txn->commit();
    if (ret != 0) {
        ret = -3;
        msg = "failed to save rowset meta";
        return;
    }
}

void MetaServiceImpl::get_rowset(::google::protobuf::RpcController* controller,
                                 const ::selectdb::GetRowsetRequest* request,
                                 ::selectdb::GetRowsetResponse* response,
                                 ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request: " << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    std::string msg = "OK";
    std::unique_ptr<int, std::function<void(int*)>> defer_status((int*)0x01, [&ret, &msg, &response,
                                                                              &ctrl](int*) {
        response->mutable_status()->set_code(ret);
        response->mutable_status()->set_msg(msg);
        LOG(INFO) << "finish " << __PRETTY_FUNCTION__ << " " << ctrl->remote_side() << " " << msg;
    });

    int64_t tablet_id = request->tablet_id();
    int64_t start = request->start_version();
    int64_t end = request->end_version();
    end = end < 0 ? std::numeric_limits<int64_t>::max() - 1 : end;

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);

    // TODO: validate request
    MetaRowsetKeyInfo key_info0 {"instance_id_deadbeef", tablet_id, start, ""};
    MetaRowsetKeyInfo key_info1 {"instance_id_deadbeef", tablet_id, end + 1, ""};
    std::string key0;
    std::string key1;
    meta_rowset_key(key_info0, &key0);
    meta_rowset_key(key_info1, &key1);
    std::unique_ptr<RangeGetIterator> it;
    ret = txn->get(key0, key1, &it);
    if (ret != 0) {
        msg = "failed to get rowset";
        msg += (ret == 1 ? ": not found" : "");
        return;
    }

    while (it->has_next()) {
        auto [k, v] = it->next();
        LOG(INFO) << "xxx key " << hex(k);
        std::string val(v.data(), v.size());
        auto rs = response->add_rowset_meta();
        if (!rs->ParseFromString(val)) {
            ret = -3;
            msg = "malformed tablet meta, unable to initialize";
            return;
        }
    }
}

} // namespace selectdb
// vim: et ts=4 sw=4 cc=80:
