
// clang-format off
#include "meta_service.h"

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
    LOG(INFO) << "rpc from " <<  ctrl->remote_side() << " request: " << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    std::unique_ptr<Transaction> txn;
    int ret = 0;
    std::string msg = "OK";
    std::unique_ptr<int, std::function<void(int*)>> defer_status((int*)0x01,
        [&ret, &msg, &response](int*) {
            response->mutable_status()->set_code(ret);
            response->mutable_status()->set_msg(msg);
    });

    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) msg = "failed to create txn";
    ret = txn->begin();
    if (ret != 0) msg = "failed to begin txn";
    txn->atomic_add("version", 1);
    ret = txn->commit();
    if (ret != 0) msg = "failed to commit txn";

    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) msg = "failed to create txn1";
    std::string val;
    txn->get("version", &val);

    int64_t version = *reinterpret_cast<int64_t*>(val.data());
    response->set_version(version);
    LOG(INFO) << "rpc from " <<  ctrl->remote_side() << " response: " << response->DebugString();
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
                                 ::google::protobuf::Closure* done) {
}

} // namespace selectdb
// vim: et ts=4 sw=4 cc=80:
