
#pragma once

#include "gen_cpp/selectdb_cloud.pb.h"
#include "meta-service/txn_kv.h"

namespace selectdb {

class MetaServiceImpl : public selectdb::MetaService {
public:
    MetaServiceImpl(std::shared_ptr<TxnKv> txn_kv);
    ~MetaServiceImpl() override;

    void begin_txn(::google::protobuf::RpcController* controller,
                   const ::selectdb::BeginTxnRequest* request,
                   ::selectdb::BeginTxnResponse* response,
                   ::google::protobuf::Closure* done) override;

    void precommit_txn(::google::protobuf::RpcController* controller,
                       const ::selectdb::PrecommitTxnRequest* request,
                       ::selectdb::PrecommitTxnResponse* response,
                       ::google::protobuf::Closure* done) override;

    void commit_txn(::google::protobuf::RpcController* controller,
                    const ::selectdb::CommitTxnRequest* request,
                    ::selectdb::CommitTxnResponse* response,
                    ::google::protobuf::Closure* done) override;

    void abort_txn(::google::protobuf::RpcController* controller,
                   const ::selectdb::AbortTxnRequest* request,
                   ::selectdb::AbortTxnResponse* response,
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

    void http(::google::protobuf::RpcController* controller,
              const ::selectdb::MetaServiceHttpRequest* request,
              ::selectdb::MetaServiceHttpResponse* response,
              ::google::protobuf::Closure* done) override;

    void get_obj_store_info(google::protobuf::RpcController* controller,
                            const ::selectdb::GetObjStoreInfoRequest* request,
                            ::selectdb::GetObjStoreInfoResponse* response,
                            ::google::protobuf::Closure* done) override;

    void create_instance(google::protobuf::RpcController* controller,
                         const ::selectdb::CreateInstanceRequest* request,
                         ::selectdb::MetaServiceGenericResponse* response,
                         ::google::protobuf::Closure* done) override;

    void add_cluster(google::protobuf::RpcController* controller,
                     const ::selectdb::AddClusterRequest* request,
                     ::selectdb::MetaServiceGenericResponse* response,
                     ::google::protobuf::Closure* done) override;

    void get_cluster(google::protobuf::RpcController* controller,
                     const ::selectdb::GetClusterRequest* request,
                     ::selectdb::GetClusterResponse* response,
                     ::google::protobuf::Closure* done) override;

private:
    std::shared_ptr<TxnKv> txn_kv_;
};

} // namespace selectdb
// vim: et tw=120 ts=4 sw=4 cc=80:
