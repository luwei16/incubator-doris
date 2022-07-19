#include "meta_service.h"

namespace selectdb {

MetaServiceImpl::MetaServiceImpl() {}
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
                                  ::google::protobuf::Closure* done) {}
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
