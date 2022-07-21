
// clang-format off
#include "meta_service.h"
#include "meta-service/keys.h"

#include "brpc/closure_guard.h"
#include "brpc/controller.h"

#include <memory>
// clang-format on

namespace selectdb {

MetaServiceImpl::MetaServiceImpl(std::shared_ptr<TxnKv> txn_kv) {
    txn_kv_ = txn_kv;
}

MetaServiceImpl::~MetaServiceImpl() {}

void MetaServiceImpl::begin_txn(::google::protobuf::RpcController* controller,
                                const ::selectdb::TxnRequest* request,
                                ::selectdb::TxnResponse* response,
                                ::google::protobuf::Closure* done) {
    // TBD
}

void MetaServiceImpl::precommit_txn(::google::protobuf::RpcController* controller,
                                    const ::selectdb::TxnRequest* request,
                                    ::selectdb::TxnResponse* response,
                                    ::google::protobuf::Closure* done) {}
void MetaServiceImpl::commit_txn(::google::protobuf::RpcController* controller,
                                 const ::selectdb::TxnRequest* request,
                                 ::selectdb::TxnResponse* response,
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
                                    ::google::protobuf::Closure* done) {}
void MetaServiceImpl::drop_tablet(::google::protobuf::RpcController* controller,
                                  const ::selectdb::DropTabletRequest* request,
                                  ::selectdb::MetaServiceGenericResponse* response,
                                  ::google::protobuf::Closure* done) {}
void MetaServiceImpl::get_tablet(::google::protobuf::RpcController* controller,
                                 const ::selectdb::GetTabletRequest* request,
                                 ::selectdb::GetTabletResponse* response,
                                 ::google::protobuf::Closure* done) {}
void MetaServiceImpl::create_rowset(::google::protobuf::RpcController* controller,
                                    const ::selectdb::CreateRowsetRequest* request,
                                    ::selectdb::MetaServiceGenericResponse* response,
                                    ::google::protobuf::Closure* done) {}

void MetaServiceImpl::get_rowset(::google::protobuf::RpcController* controller,
                                 const ::selectdb::GetRowsetRequest* request,
                                 ::selectdb::GetRowsetResponse* response,
                                 ::google::protobuf::Closure* done) {}

} // namespace selectdb
// vim: et ts=4 sw=4 cc=80:
