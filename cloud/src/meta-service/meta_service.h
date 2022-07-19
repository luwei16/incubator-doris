
#pragma once

#include "gen_cpp/selectdb_cloud.pb.h"

namespace selectdb {

class MetaServiceImpl : public selectdb::MetaService {
public:
    MetaServiceImpl();
    virtual ~MetaServiceImpl();

    void begin_txn(::google::protobuf::RpcController* controller,
                   const ::selectdb::TxnRequest* request, ::selectdb::TxnResponse* response,
                   ::google::protobuf::Closure* done) override;
    void precommit_txn(::google::protobuf::RpcController* controller,
                       const ::selectdb::TxnRequest* request, ::selectdb::TxnResponse* response,
                       ::google::protobuf::Closure* done) override;
    void commit_txn(::google::protobuf::RpcController* controller,
                    const ::selectdb::TxnRequest* request, ::selectdb::TxnResponse* response,
                    ::google::protobuf::Closure* done) override;
    void get_version(::google::protobuf::RpcController* controller,
                     const ::selectdb::GetVersionRequest* request,
                     ::selectdb::GetVersionResponse* response,
                     ::google::protobuf::Closure* done) override;
    void create_tablet(::google::protobuf::RpcController* controller,
                       const ::selectdb::CreateTabletRequest* request,
                       ::selectdb::MetaServiceGenericResponse* response,
                       ::google::protobuf::Closure* done) override;
    void drop_tablet(::google::protobuf::RpcController* controller,
                     const ::selectdb::DropTabletRequest* request,
                     ::selectdb::MetaServiceGenericResponse* response,
                     ::google::protobuf::Closure* done) override;
    void get_tablet(::google::protobuf::RpcController* controller,
                    const ::selectdb::GetTabletRequest* request,
                    ::selectdb::GetTabletResponse* response,
                    ::google::protobuf::Closure* done) override;
    void create_rowset(::google::protobuf::RpcController* controller,
                       const ::selectdb::CreateRowsetRequest* request,
                       ::selectdb::MetaServiceGenericResponse* response,
                       ::google::protobuf::Closure* done) override;
    void get_rowset(::google::protobuf::RpcController* controller,
                    const ::selectdb::GetRowsetRequest* request,
                    ::selectdb::GetRowsetResponse* response,
                    ::google::protobuf::Closure* done) override;
};

} // namespace selectdb
// vim: et tw=120 ts=4 sw=4 cc=80:
