
//clang-format off
#include "meta-service/meta_service.h"

#include "common/config.h"
#include "common/sync_point.h"
#include "common/util.h"
#include "gen_cpp/selectdb_cloud.pb.h"
#include "meta-service/keys.h"
#include "meta-service/mem_txn_kv.h"
#include "resource-manager/resource_manager.h"

#include "brpc/controller.h"
#include "gtest/gtest.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <random>
// clang-format on

int main(int argc, char** argv) {
    selectdb::config::init(nullptr, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

std::string mock_instance = "test_instance";
std::string mock_cluster_name = "test_cluster";
std::string mock_cluster_id = "test_cluster_id";
using namespace selectdb;
class MockResourceManager : public ResourceManager {
public:
    MockResourceManager(std::shared_ptr<TxnKv> txn_kv) : ResourceManager(txn_kv) {};
    ~MockResourceManager() override = default;

    int init() override { return 0; }

    std::string get_node(const std::string& cloud_unique_id,
                         std::vector<NodeInfo>* nodes) override {
        NodeInfo i {Role::COMPUTE_NODE, mock_instance, mock_cluster_name, mock_cluster_id};
        nodes->push_back(i);
        return "";
    }

    std::string add_cluster(const std::string& instance_id, const ClusterInfo& cluster) override {
        return "";
    }

    std::string drop_cluster(const std::string& instance_id, const ClusterInfo& cluster) override {
        return "";
    }

    std::string update_cluster(const std::string& instance_id, const ClusterInfo& cluster,
                               std::function<std::string(::selectdb::ClusterPB&)> action,
                               std::function<bool(const ::selectdb::ClusterPB&)> filter) override {
        return "";
    }

    std::pair<int, std::string> get_instance(std::shared_ptr<Transaction> txn,
                                             const std::string& instance_id,
                                             InstanceInfoPB* inst_pb) override {
        return {1, ""};
    }
};

std::unique_ptr<MetaServiceImpl> get_meta_service() {
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    [&] { ASSERT_NE(txn_kv.get(), nullptr); }();
    auto rs = std::make_shared<MockResourceManager>(txn_kv);
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs);
    return meta_service;
}

TEST(MetaServiceTest, CreateInstanceTest) {
    auto meta_service = get_meta_service();

    // case: normal create instance
    {
        brpc::Controller cntl;
        CreateInstanceRequest req;
        req.set_instance_id("test_instance");
        req.set_user_id("test_user");
        req.set_name("test_name");
        ObjectStoreInfoPB obj;
        obj.set_ak("123");
        obj.set_sk("321");
        obj.set_bucket("456");
        obj.set_prefix("654");
        obj.set_endpoint("789");
        obj.set_region("987");
        req.mutable_obj_info()->CopyFrom(obj);

        MetaServiceGenericResponse res;
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // case: request has invalid argument
    {
        brpc::Controller cntl;
        CreateInstanceRequest req;
        MetaServiceGenericResponse res;
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }
}

TEST(MetaServiceTest, AlterClusterTest) {
    auto meta_service = get_meta_service();
    ASSERT_NE(meta_service, nullptr);

    // case: normal add cluster
    {
        brpc::Controller cntl;
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_name(mock_cluster_name);
        req.set_op(AlterClusterRequest::ADD_CLUSTER);
        MetaServiceGenericResponse res;
        meta_service->alter_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // case: request has invalid argument
    {
        brpc::Controller cntl;
        AlterClusterRequest req;
        req.set_op(AlterClusterRequest::DROP_CLUSTER);
        MetaServiceGenericResponse res;
        meta_service->alter_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }
}

TEST(MetaServiceTest, GetClusterTest) {
    auto meta_service = get_meta_service();

    // add cluster first
    InstanceKeyInfo key_info {mock_instance};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    InstanceInfoPB instance;
    instance.set_instance_id(mock_instance);
    ClusterPB c1;
    c1.set_cluster_name(mock_cluster_name);
    c1.set_cluster_id(mock_cluster_id);
    c1.add_mysql_user_name()->append("m1");
    instance.add_clusters()->CopyFrom(c1);
    val = instance.SerializeAsString();

    std::unique_ptr<Transaction> txn;
    std::string get_val;
    int ret = meta_service->txn_kv_->create_txn(&txn);
    ASSERT_EQ(ret, 0);
    txn->put(key, val);
    ASSERT_EQ(txn->commit(), 0);

    // case: normal get
    {
        brpc::Controller cntl;
        GetClusterRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_cluster_id(mock_cluster_id);
        req.set_cluster_name("test_cluster");
        GetClusterResponse res;
        meta_service->get_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                  &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
}

TEST(MetaServiceTest, GetTabletStatsTest) {
    auto meta_service = get_meta_service();

    // add tablet first
    StatsTabletKeyInfo stat_key_info {mock_instance, 1, 2, 3, 4};
    std::string key, val;
    stats_tablet_key(stat_key_info, &key);

    TabletStatsPB stat;
    stat.mutable_idx()->set_table_id(1);
    stat.mutable_idx()->set_index_id(2);
    stat.mutable_idx()->set_partition_id(3);
    stat.mutable_idx()->set_tablet_id(4);
    val = stat.SerializeAsString();

    std::unique_ptr<Transaction> txn;
    int ret = meta_service->txn_kv_->create_txn(&txn);
    ASSERT_EQ(ret, 0);
    txn->put(key, val);
    ASSERT_EQ(txn->commit(), 0);

    // case: normal get
    {
        brpc::Controller cntl;
        GetTabletStatsRequest req;
        req.add_tablet_idx()->CopyFrom(stat.idx());
        GetTabletStatsResponse res;
        meta_service->get_tablet_stats(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
}

TEST(MetaServiceTest, BeginTxnTest) {
    auto meta_service = get_meta_service();

    // case: label not exist previously
    {
        brpc::Controller cntl;
        BeginTxnRequest req;

        req.set_cloud_unique_id("test_cloud_unique_id");
        TxnInfoPB txn_info_pb;
        txn_info_pb.set_db_id(666);
        txn_info_pb.set_label("test_label");
        req.mutable_txn_info()->CopyFrom(txn_info_pb);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // case: label already used
    {
        brpc::Controller cntl;
        BeginTxnRequest req;

        req.set_cloud_unique_id("test_cloud_unique_id");
        TxnInfoPB txn_info_pb;
        txn_info_pb.set_db_id(888);
        txn_info_pb.set_label("test_label_2");
        req.mutable_txn_info()->CopyFrom(txn_info_pb);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_LABEL_ALREADY_USED);
    }

    // case: dup begin txn request
    {
        brpc::Controller cntl;
        BeginTxnRequest req;

        req.set_cloud_unique_id("test_cloud_unique_id");
        TxnInfoPB txn_info_pb;
        txn_info_pb.set_db_id(999);
        txn_info_pb.set_label("test_label_3");
        UniqueIdPB unique_id_pb;
        unique_id_pb.set_hi(100);
        unique_id_pb.set_lo(10);
        txn_info_pb.mutable_request_id()->CopyFrom(unique_id_pb);
        req.mutable_txn_info()->CopyFrom(txn_info_pb);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_DUPLICATED_REQ);
    }
}

TEST(MetaServiceTest, PreCommitTxnTest) {
    auto meta_service = get_meta_service();

    // begin txn first
    brpc::Controller cntl;
    BeginTxnRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    TxnInfoPB txn_info_pb;
    txn_info_pb.set_db_id(666);
    txn_info_pb.set_label("test_label");
    req.mutable_txn_info()->CopyFrom(txn_info_pb);
    BeginTxnResponse res;
    meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                            nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    int64_t txn_id = res.txn_id();

    // case: txn's status should be TXN_STATUS_PRECOMMITTED
    {
        std::unique_ptr<Transaction> txn;
        int ret = meta_service->txn_kv_->create_txn(&txn);
        ASSERT_EQ(ret, 0);
        std::string txn_inf_key;
        std::string txn_inf_val;
        TxnInfoKeyInfo txn_inf_key_info {mock_instance, 666, txn_id};
        txn_info_key(txn_inf_key_info, &txn_inf_key);
        ASSERT_EQ(txn->get(txn_inf_key, &txn_inf_val), 0);
        TxnInfoPB txn_info;
        txn_info.ParseFromString(txn_inf_val);
        // before call precommit_txn, txn's status is TXN_STATUS_PREPARED
        ASSERT_EQ(txn_info.status(), TxnStatusPB::TXN_STATUS_PREPARED);

        brpc::Controller cntl;
        PrecommitTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_db_id(666);
        req.set_txn_id(txn_id);
        PrecommitTxnResponse res;
        meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        ret = meta_service->txn_kv_->create_txn(&txn);
        ASSERT_EQ(ret, 0);
        ASSERT_EQ(txn->get(txn_inf_key, &txn_inf_val), 0);
        txn_info.ParseFromString(txn_inf_val);
        // after call precommit_txn, txn's status is TXN_STATUS_PRECOMMITTED
        ASSERT_EQ(txn_info.status(), TxnStatusPB::TXN_STATUS_PRECOMMITTED);
    }

    // case: when txn's status is TXN_STATUS_ABORTED/TXN_STATUS_VISIBLE/TXN_STATUS_PRECOMMITTED
    {
        // TXN_STATUS_ABORTED
        std::unique_ptr<Transaction> txn;
        int ret = meta_service->txn_kv_->create_txn(&txn);
        ASSERT_EQ(ret, 0);
        std::string txn_inf_key;
        std::string txn_inf_val;
        TxnInfoKeyInfo txn_inf_key_info {mock_instance, 666, txn_id};
        txn_info_key(txn_inf_key_info, &txn_inf_key);
        ASSERT_EQ(txn->get(txn_inf_key, &txn_inf_val), 0);
        TxnInfoPB txn_info;
        txn_info.ParseFromString(txn_inf_val);
        txn_info.set_status(TxnStatusPB::TXN_STATUS_ABORTED);
        txn_inf_val.clear();
        txn_info.SerializeToString(&txn_inf_val);
        txn->put(txn_inf_key, txn_inf_val);
        ASSERT_EQ(ret = txn->commit(), 0);

        brpc::Controller cntl;
        PrecommitTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_db_id(666);
        req.set_txn_id(txn_id);
        PrecommitTxnResponse res;
        meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_ALREADY_ABORTED);

        // TXN_STATUS_VISIBLE
        txn_info.set_status(TxnStatusPB::TXN_STATUS_VISIBLE);
        txn_inf_val.clear();
        txn_info.SerializeToString(&txn_inf_val);
        txn->put(txn_inf_key, txn_inf_val);
        ASSERT_EQ(ret = txn->commit(), 0);
        meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_ALREADY_VISIBLE);

        // TXN_STATUS_PRECOMMITTED
        txn_info.set_status(TxnStatusPB::TXN_STATUS_PRECOMMITTED);
        txn_inf_val.clear();
        txn_info.SerializeToString(&txn_inf_val);
        txn->put(txn_inf_key, txn_inf_val);
        ASSERT_EQ(ret = txn->commit(), 0);
        meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_ALREADY_PRECOMMITED);
    }
}

static int64_t cnt = 0;
static void create_tmp_rowset_and_meta_tablet(TxnKv* txn_kv, int64_t txn_id, int64_t tablet_id,
                                              int table_id = 1,
                                              std::string instance_id = mock_instance,
                                              int num_segments = 1) {
    ++cnt;

    std::string key;
    std::string val;

    char rowset_id[50];
    snprintf(rowset_id, sizeof(rowset_id), "%048ld", cnt);

    MetaRowsetTmpKeyInfo key_info {instance_id, txn_id, tablet_id};
    meta_rowset_tmp_key(key_info, &key);

    doris::RowsetMetaPB rowset_pb;
    rowset_pb.set_rowset_id(0); // useless but required
    rowset_pb.set_rowset_id_v2(rowset_id);
    rowset_pb.set_tablet_id(tablet_id);
    rowset_pb.set_num_segments(num_segments);
    rowset_pb.SerializeToString(&val);

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->put(key, val);
    ASSERT_EQ(txn->commit(), 0);

    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    std::string key1;
    std::string val1;
    MetaTabletIdxKeyInfo key_info1 {instance_id, tablet_id};
    meta_tablet_idx_key(key_info1, &key1);
    TabletIndexPB tablet_table;
    tablet_table.set_table_id(1);
    tablet_table.set_index_id(2);
    tablet_table.set_partition_id(3);
    tablet_table.SerializeToString(&val1);

    txn->put(key1, val1);
    ASSERT_EQ(txn->commit(), 0);
}

TEST(MetaServiceTest, CommitTxnTest) {
    auto meta_service = get_meta_service();

    // case: first version of rowset
    {
        int64_t txn_id = -1;
        // begin txn
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(666);
            txn_info_pb.set_label("test_label");
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id = res.txn_id();
        }

        // mock rowset and tablet
        int64_t tablet_id_base = 1103;
        for (int i = 0; i < 5; ++i) {
            create_tmp_rowset_and_meta_tablet(meta_service->txn_kv_.get(), txn_id,
                                              tablet_id_base + i);
        }

        // precommit txn
        {
            brpc::Controller cntl;
            PrecommitTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(666);
            req.set_txn_id(txn_id);
            PrecommitTxnResponse res;
            meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        // commit txn
        {
            brpc::Controller cntl;
            CommitTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(666);
            req.set_txn_id(txn_id);
            CommitTxnResponse res;
            meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }
    }
}

TEST(MetaServiceTest, TabletJobTest) {
    auto meta_service = get_meta_service();
    meta_service->resource_mgr_.reset(); // Do not use resource manager

    std::string instance_id = "tablet_job_test_instance_id";

    [[maybe_unused]] auto sp = SyncPoint::get_instance();
    sp->set_call_back("get_instance_id::pred", [](void* p) { *((bool*)p) = true; });
    sp->set_call_back("get_instance_id", [&](void* p) { *((std::string*)p) = instance_id; });
    sp->enable_processing();

    brpc::Controller cntl;

    // Start compaciton job
    {
        StartTabletJobRequest req;
        StartTabletJobResponse res;

        req.mutable_job()->mutable_compaction()->set_initiator("ip:port");
        meta_service->start_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("no valid tablet_id given"), std::string::npos);

        int64_t tablet_id = 4;
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        meta_service->start_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("failed to get table id with tablet_id"),
                  std::string::npos);

        auto index_key = meta_tablet_idx_key({instance_id, tablet_id});
        TabletIndexPB idx_pb;
        idx_pb.set_table_id(1);
        idx_pb.set_index_id(2);
        idx_pb.set_partition_id(3);
        idx_pb.set_tablet_id(tablet_id + 1); // error
        std::string idx_val = idx_pb.SerializeAsString();
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        txn->put(index_key, idx_val);
        ASSERT_EQ(txn->commit(), 0);
        meta_service->start_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("internal error"), std::string::npos);

        idx_pb.set_tablet_id(tablet_id); // Correct tablet_id
        idx_val = idx_pb.SerializeAsString();
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        txn->put(index_key, idx_val);
        ASSERT_EQ(txn->commit(), 0);
        meta_service->start_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        auto job_key = job_tablet_key({instance_id, idx_pb.table_id(), idx_pb.index_id(),
                                       idx_pb.partition_id(), idx_pb.tablet_id()});
        std::string job_val;
        ASSERT_EQ(txn->get(job_key, &job_val), 0);
        TabletJobInfoPB job_pb;
        ASSERT_TRUE(job_pb.ParseFromString(job_val));

        LOG(INFO) << proto_to_json(job_pb, true);

        meta_service->start_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("job already started"), std::string::npos);
    }

    // Finish, this unit test relies on the previous (start)
    {
        FinishTabletJobRequest req;
        FinishTabletJobResponse res;

        req.mutable_job()->mutable_compaction()->set_initiator("ip:port");
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("no valid tablet_id given"), std::string::npos);

        int64_t table_id = 1;
        int64_t index_id = 2;
        int64_t partition_id = 3;
        int64_t tablet_id = 4;
        req.mutable_job()->mutable_idx()->set_table_id(table_id);
        req.mutable_job()->mutable_idx()->set_index_id(index_id);
        req.mutable_job()->mutable_idx()->set_partition_id(partition_id);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        // Action is not set
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("unsupported action"), std::string::npos);

        req.set_action(FinishTabletJobRequest::COMMIT);

        // Tablet meta not found, this is unexpected
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("failed to get tablet meta"), std::string::npos);

        // Create txn meta, and try again
        auto tablet_meta_key =
                meta_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        doris::TabletMetaPB tablet_meta_pb;
        tablet_meta_pb.set_table_id(table_id);
        tablet_meta_pb.set_index_id(index_id);
        tablet_meta_pb.set_partition_id(partition_id);
        tablet_meta_pb.set_tablet_id(tablet_id);
        tablet_meta_pb.set_cumulative_layer_point(50);
        std::string tablet_meta_val = tablet_meta_pb.SerializeAsString();
        ASSERT_FALSE(tablet_meta_val.empty());
        txn->put(tablet_meta_key, tablet_meta_val);
        ASSERT_EQ(txn->commit(), 0);

        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);

        // Create create tablet stats, compation job will will update stats
        auto tablet_stats_key =
                stats_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        TabletStatsPB tablet_stats_pb;
        tablet_stats_pb.mutable_idx()->set_table_id(table_id);
        tablet_stats_pb.mutable_idx()->set_index_id(index_id);
        tablet_stats_pb.mutable_idx()->set_partition_id(partition_id);
        tablet_stats_pb.mutable_idx()->set_tablet_id(tablet_id);

        std::mt19937 rng(std::chrono::system_clock::now().time_since_epoch().count());
        std::uniform_int_distribution<int> dist(1, 10000); // Positive numbers
        tablet_stats_pb.set_cumulative_compaction_cnt(dist(rng));
        tablet_stats_pb.set_base_compaction_cnt(dist(rng));
        tablet_stats_pb.set_cumulative_point(tablet_meta_pb.cumulative_layer_point());
        tablet_stats_pb.set_num_rows(dist(rng));
        tablet_stats_pb.set_data_size(dist(rng));
        tablet_stats_pb.set_num_rowsets(dist(rng));
        tablet_stats_pb.set_num_segments(dist(rng));
        tablet_stats_pb.set_last_compaction_time(dist(rng));

        req.mutable_job()->mutable_compaction()->set_output_cumulative_point(
                tablet_stats_pb.cumulative_point() + dist(rng));
        req.mutable_job()->mutable_compaction()->set_num_output_rows(dist(rng));
        req.mutable_job()->mutable_compaction()->set_num_output_rowsets(dist(rng));
        req.mutable_job()->mutable_compaction()->set_num_output_segments(dist(rng));
        req.mutable_job()->mutable_compaction()->set_num_input_rows(dist(rng));
        req.mutable_job()->mutable_compaction()->set_num_input_rowsets(dist(rng));
        req.mutable_job()->mutable_compaction()->set_num_input_segments(dist(rng));
        req.mutable_job()->mutable_compaction()->set_size_input_rowsets(dist(rng));
        req.mutable_job()->mutable_compaction()->set_size_output_rowsets(dist(rng));
        req.mutable_job()->mutable_compaction()->set_type(
                (dist(rng) % 10) < 2 ? TabletCompactionJobPB::BASE
                                     : TabletCompactionJobPB::CUMULATIVE);

        std::string tablet_stats_val = tablet_stats_pb.SerializeAsString();
        ASSERT_FALSE(tablet_stats_val.empty());
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        txn->put(tablet_stats_key, tablet_stats_val);
        ASSERT_EQ(txn->commit(), 0);

        // Input rowset not valid
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("invalid input"), std::string::npos);

        // Provide input and output rowset info
        int64_t input_version_start = dist(rng);
        int64_t input_version_end = input_version_start + 100;
        req.mutable_job()->mutable_compaction()->add_input_versions(input_version_start);
        req.mutable_job()->mutable_compaction()->add_input_versions(input_version_end);
        req.mutable_job()->mutable_compaction()->add_output_versions(input_version_end);
        req.mutable_job()->mutable_compaction()->add_output_rowset_ids("output rowset id");

        // Input rowsets must exist, and more than 1
        // Check number input rowsets
        sp->set_call_back("process_compaction_job::loop_input_done", [](void* c) {
            int& num_input_rowsets = *(int*)c;
            ASSERT_EQ(num_input_rowsets, 0); // zero existed rowsets
        });
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("too few input rowsets"), std::string::npos);

        // Provide input rowset KVs, boundary test, 5 input rowsets
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        // clang-format off
        std::vector<std::string> input_rowset_keys = {
                meta_rowset_key({instance_id, tablet_id, input_version_start - 1}),
                meta_rowset_key({instance_id, tablet_id, input_version_start}),
                meta_rowset_key({instance_id, tablet_id, input_version_start + 1}),
                meta_rowset_key({instance_id, tablet_id, (input_version_start + input_version_end) / 2}),
                meta_rowset_key({instance_id, tablet_id, input_version_end - 1}),
                meta_rowset_key({instance_id, tablet_id, input_version_end}),
                meta_rowset_key({instance_id, tablet_id, input_version_end + 1}),
        };
        // clang-format on
        std::vector<std::unique_ptr<std::string>> input_rowset_vals;
        for (auto& i : input_rowset_keys) {
            doris::RowsetMetaPB rs_pb;
            rs_pb.set_rowset_id(0);
            rs_pb.set_rowset_id_v2(hex(i));
            input_rowset_vals.emplace_back(new std::string(rs_pb.SerializeAsString()));
            txn->put(i, *input_rowset_vals.back());
        }
        ASSERT_EQ(txn->commit(), 0);

        // Check number input rowsets
        sp->set_call_back("process_compaction_job::loop_input_done", [](void* c) {
            int& num_input_rowsets = *(int*)c;
            ASSERT_EQ(num_input_rowsets, 5);
        });
        // No tmp rowset key (output rowset)
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("invalid txn_id"), std::string::npos);

        int64_t txn_id = dist(rng);
        req.mutable_job()->mutable_compaction()->set_txn_id(txn_id);

        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("failed to get tmp rowset key"), std::string::npos);

        // Provide invalid output rowset meta
        auto tmp_rowset_key = meta_rowset_tmp_key({instance_id, txn_id, tablet_id});
        doris::RowsetMetaPB tmp_rs_pb;
        tmp_rs_pb.set_rowset_id(0);
        auto tmp_rowset_val = tmp_rs_pb.SerializeAsString();
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        txn->put(tmp_rowset_key, tmp_rowset_val);
        ASSERT_EQ(txn->commit(), 0);

        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("invalid txn_id in output tmp rowset meta"), std::string::npos);

        // Provide txn_id in output rowset meta
        tmp_rs_pb.set_txn_id(10086);
        tmp_rowset_val = tmp_rs_pb.SerializeAsString();
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        txn->put(tmp_rowset_key, tmp_rowset_val);
        ASSERT_EQ(txn->commit(), 0);

        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        //=====================================================================
        // All branch tests done, we are done commit a compaction job
        //=====================================================================
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        tablet_stats_val.clear();
        ASSERT_EQ(txn->get(tablet_stats_key, &tablet_stats_val), 0);
        TabletStatsPB stats;
        ASSERT_TRUE(stats.ParseFromString(tablet_stats_val));

        // clang-format off
        EXPECT_EQ(stats.base_compaction_cnt()      , tablet_stats_pb.base_compaction_cnt() + (req.job().compaction().type() == TabletCompactionJobPB::BASE));
        EXPECT_EQ(stats.cumulative_compaction_cnt(), tablet_stats_pb.cumulative_compaction_cnt() + (req.job().compaction().type() == TabletCompactionJobPB::CUMULATIVE));
        EXPECT_EQ(stats.cumulative_point()         , req.job().compaction().output_cumulative_point());
        EXPECT_EQ(stats.num_rows()                 , tablet_stats_pb.num_rows() + (req.job().compaction().num_output_rows() - req.job().compaction().num_input_rows()));
        EXPECT_EQ(stats.data_size()                , tablet_stats_pb.data_size() + (req.job().compaction().size_output_rowsets() - req.job().compaction().size_input_rowsets()));
        EXPECT_EQ(stats.num_rowsets()              , tablet_stats_pb.num_rowsets() + (req.job().compaction().num_output_rowsets() - req.job().compaction().num_input_rowsets()));
        EXPECT_EQ(stats.num_segments()             , tablet_stats_pb.num_segments() + (req.job().compaction().num_output_segments() - req.job().compaction().num_input_segments()));
        // EXPECT_EQ(stats.last_compaction_time     (), now);
        // clang-format on

        // Check job removed, tablet meta updated
        auto job_key = job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        std::string job_val;
        ASSERT_EQ(txn->get(job_key, &job_val), 1);
        tablet_meta_val.clear();
        ASSERT_EQ(txn->get(tablet_meta_key, &tablet_meta_val), 0);
        ASSERT_TRUE(tablet_meta_pb.ParseFromString(tablet_meta_val));
        LOG(INFO) << tablet_meta_pb.DebugString();
        ASSERT_EQ(tablet_meta_pb.cumulative_layer_point(),
                  req.job().compaction().output_cumulative_point());
        ASSERT_EQ(tablet_meta_pb.cumulative_layer_point(), stats.cumulative_point());

        // Check tmp rowset removed
        ASSERT_EQ(txn->get(tmp_rowset_key, &tmp_rowset_val), 1);
        // Check input rowsets removed
        for (int i = 1; i < input_rowset_keys.size() - 1; ++i) {
            std::string val;
            ASSERT_EQ(txn->get(input_rowset_keys[i], &val), 1);
        }
        // Check recycle rowsets added
        for (int i = 1; i < input_rowset_vals.size() - 1; ++i) {
            doris::RowsetMetaPB rs;
            ASSERT_TRUE(rs.ParseFromString(*input_rowset_vals[i]));
            auto key = recycle_rowset_key({instance_id, tablet_id, rs.rowset_id_v2()});
            std::string val;
            EXPECT_EQ(txn->get(key, &val), 0) << hex(key);
        }
        // Check output rowset added
        auto rowset_key = meta_rowset_key({instance_id, tablet_id, input_version_end});
        std::string rowset_val;
        EXPECT_EQ(txn->get(rowset_key, &rowset_val), 0) << hex(rowset_key);
    }
}
// vim: et tw=100 ts=4 sw=4 cc=80:
