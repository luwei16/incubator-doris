
#pragma once

#include "gen_cpp/selectdb_cloud.pb.h"
#include "meta-service/txn_kv.h"
#include "resource-manager/resource_manager.h"

namespace selectdb {

class Transaction;
class MetaServiceImpl : public selectdb::MetaService {
public:
    MetaServiceImpl(std::shared_ptr<TxnKv> txn_kv, std::shared_ptr<ResourceManager> resource_mgr);
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

    // clang-format off
    void get_txn(::google::protobuf::RpcController* controller,
                 const ::selectdb::GetTxnRequest* request,
                 ::selectdb::GetTxnResponse* response,
                 ::google::protobuf::Closure* done) override;
    // clang-format on

    void get_current_max_txn_id(::google::protobuf::RpcController* controller,
                                const ::selectdb::GetCurrentMaxTxnRequest* request,
                                ::selectdb::GetCurrentMaxTxnResponse* response,
                                ::google::protobuf::Closure* done) override;

    void check_txn_conflict(::google::protobuf::RpcController* controller,
                            const ::selectdb::CheckTxnConflictRequest* request,
                            ::selectdb::CheckTxnConflictResponse* response,
                            ::google::protobuf::Closure* done) override;

    void get_version(::google::protobuf::RpcController* controller,
                     const ::selectdb::GetVersionRequest* request,
                     ::selectdb::GetVersionResponse* response,
                     ::google::protobuf::Closure* done) override;

    void create_tablet(::google::protobuf::RpcController* controller,
                       const ::selectdb::CreateTabletRequest* request,
                       ::selectdb::MetaServiceGenericResponse* response,
                       ::google::protobuf::Closure* done) override;

    void update_tablet(::google::protobuf::RpcController* controller,
                       const ::selectdb::UpdateTabletRequest* request,
                       ::selectdb::MetaServiceGenericResponse* response,
                       ::google::protobuf::Closure* done) override;

    void get_tablet(::google::protobuf::RpcController* controller,
                    const ::selectdb::GetTabletRequest* request,
                    ::selectdb::GetTabletResponse* response,
                    ::google::protobuf::Closure* done) override;

    void prepare_rowset(::google::protobuf::RpcController* controller,
                        const ::selectdb::CreateRowsetRequest* request,
                        ::selectdb::MetaServiceGenericResponse* response,
                        ::google::protobuf::Closure* done) override;

    void commit_rowset(::google::protobuf::RpcController* controller,
                       const ::selectdb::CreateRowsetRequest* request,
                       ::selectdb::MetaServiceGenericResponse* response,
                       ::google::protobuf::Closure* done) override;

    void get_rowset(::google::protobuf::RpcController* controller,
                    const ::selectdb::GetRowsetRequest* request,
                    ::selectdb::GetRowsetResponse* response,
                    ::google::protobuf::Closure* done) override;

    void prepare_index(::google::protobuf::RpcController* controller,
                       const ::selectdb::IndexRequest* request,
                       ::selectdb::MetaServiceGenericResponse* response,
                       ::google::protobuf::Closure* done) override;

    void commit_index(::google::protobuf::RpcController* controller,
                      const ::selectdb::IndexRequest* request,
                      ::selectdb::MetaServiceGenericResponse* response,
                      ::google::protobuf::Closure* done) override;

    void drop_index(::google::protobuf::RpcController* controller,
                    const ::selectdb::IndexRequest* request,
                    ::selectdb::MetaServiceGenericResponse* response,
                    ::google::protobuf::Closure* done) override;

    void prepare_partition(::google::protobuf::RpcController* controller,
                           const ::selectdb::PartitionRequest* request,
                           ::selectdb::MetaServiceGenericResponse* response,
                           ::google::protobuf::Closure* done) override;

    void commit_partition(::google::protobuf::RpcController* controller,
                          const ::selectdb::PartitionRequest* request,
                          ::selectdb::MetaServiceGenericResponse* response,
                          ::google::protobuf::Closure* done) override;

    void drop_partition(::google::protobuf::RpcController* controller,
                        const ::selectdb::PartitionRequest* request,
                        ::selectdb::MetaServiceGenericResponse* response,
                        ::google::protobuf::Closure* done) override;

    void get_tablet_stats(::google::protobuf::RpcController* controller,
                          const ::selectdb::GetTabletStatsRequest* request,
                          ::selectdb::GetTabletStatsResponse* response,
                          ::google::protobuf::Closure* done) override;

    void start_tablet_job(::google::protobuf::RpcController* controller,
                          const ::selectdb::StartTabletJobRequest* request,
                          ::selectdb::StartTabletJobResponse* response,
                          ::google::protobuf::Closure* done) override;

    void finish_tablet_job(::google::protobuf::RpcController* controller,
                           const ::selectdb::FinishTabletJobRequest* request,
                           ::selectdb::FinishTabletJobResponse* response,
                           ::google::protobuf::Closure* done) override;

    void http(::google::protobuf::RpcController* controller,
              const ::selectdb::MetaServiceHttpRequest* request,
              ::selectdb::MetaServiceHttpResponse* response,
              ::google::protobuf::Closure* done) override;

    void get_obj_store_info(google::protobuf::RpcController* controller,
                            const ::selectdb::GetObjStoreInfoRequest* request,
                            ::selectdb::GetObjStoreInfoResponse* response,
                            ::google::protobuf::Closure* done) override;

    void alter_obj_store_info(google::protobuf::RpcController* controller,
                              const ::selectdb::AlterObjStoreInfoRequest* request,
                              ::selectdb::MetaServiceGenericResponse* response,
                              ::google::protobuf::Closure* done) override;

    void create_instance(google::protobuf::RpcController* controller,
                         const ::selectdb::CreateInstanceRequest* request,
                         ::selectdb::MetaServiceGenericResponse* response,
                         ::google::protobuf::Closure* done) override;

    void alter_cluster(google::protobuf::RpcController* controller,
                       const ::selectdb::AlterClusterRequest* request,
                       ::selectdb::MetaServiceGenericResponse* response,
                       ::google::protobuf::Closure* done) override;

    void get_cluster(google::protobuf::RpcController* controller,
                     const ::selectdb::GetClusterRequest* request,
                     ::selectdb::GetClusterResponse* response,
                     ::google::protobuf::Closure* done) override;

    void create_stage(google::protobuf::RpcController* controller,
                      const ::selectdb::CreateStageRequest* request,
                      ::selectdb::CreateStageResponse* response,
                      ::google::protobuf::Closure* done) override;

    void get_stage(google::protobuf::RpcController* controller,
                   const ::selectdb::GetStageRequest* request,
                   ::selectdb::GetStageResponse* response,
                   ::google::protobuf::Closure* done) override;
private:
    // returns 0 for index exists, 1 for not exist, negative for error
    int index_exists(const ::selectdb::IndexRequest* request,
                     ::selectdb::MetaServiceGenericResponse* response);

    void put_recycle_index_kv(const ::selectdb::IndexRequest* request,
                              ::selectdb::MetaServiceGenericResponse* response);

    void remove_recycle_index_kv(const ::selectdb::IndexRequest* request,
                                 ::selectdb::MetaServiceGenericResponse* response);

    // returns 0 for partition exists, 1 for not exist, negative for error
    int partition_exists(const ::selectdb::PartitionRequest* request,
                         ::selectdb::MetaServiceGenericResponse* response);

    void put_recycle_partition_kv(const ::selectdb::PartitionRequest* request,
                                  ::selectdb::MetaServiceGenericResponse* response);

    void remove_recycle_partition_kv(const ::selectdb::PartitionRequest* request,
                                     ::selectdb::MetaServiceGenericResponse* response);

private:
    std::shared_ptr<TxnKv> txn_kv_;
    std::shared_ptr<ResourceManager> resource_mgr_;
};

} // namespace selectdb
// vim: et tw=120 ts=4 sw=4 cc=80:
