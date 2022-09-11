
#include <gen_cpp/selectdb_cloud.pb.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include "common/config.h"
#include "common/util.h"
#include "meta-service/keys.h"
#include "meta-service/doris_txn.h"
#include "meta-service/meta_service.h"
#include "meta-service/mem_txn_kv.h"

#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "gtest/gtest.h"
#include "resource-manager/resource_manager.h"
// clang-format on

int main(int argc, char** argv) {
    selectdb::config::init(nullptr, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

std::string mock_instance = "test_instance";
std::string mock_cluster = "test_cluster";
std::string mock_cluster_id = "test_cluster_id";
using namespace selectdb;
class MockResourceManager : public ResourceManager {
public:
    MockResourceManager(std::shared_ptr<TxnKv> txn_kv) : ResourceManager(txn_kv) {};
    ~MockResourceManager() override = default;

    int init() override {
        return 0;
    }

    std::string get_node (const std::string& cloud_unique_id, std::vector<NodeInfo>* nodes) override {
        NodeInfo i{Role::COMPUTE_NODE, mock_instance, mock_cluster, mock_cluster_id};
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

TEST(MetaServiceTest, CreateInstanceTest){
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(txn_kv.get(), nullptr);

    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, nullptr);

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
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                                    nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // case: request has invalid argument
    {
        brpc::Controller cntl;
        CreateInstanceRequest req;
        MetaServiceGenericResponse res;
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                                    nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }
}

TEST(MetaServiceTest, AlterClusterTest) {
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(txn_kv.get(), nullptr);

    auto rs = std::make_shared<MockResourceManager>(txn_kv);
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs);

    // case: normal add cluster
    {
        brpc::Controller cntl;
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_name(mock_cluster);
        req.set_op(AlterClusterRequest::ADD_CLUSTER);
        MetaServiceGenericResponse res;
        meta_service->alter_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                                    nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // case: request has invalid argument
    {
        brpc::Controller cntl;
        AlterClusterRequest req;
        req.set_op(AlterClusterRequest::DROP_CLUSTER);
        MetaServiceGenericResponse res;
        meta_service->alter_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                                    nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

}

TEST(MetaServiceTest, GetClusterTest) {
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(txn_kv.get(), nullptr);

    auto rs = std::make_shared<MockResourceManager>(txn_kv);
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs);


    // add cluster first
    InstanceKeyInfo key_info {mock_instance};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    InstanceInfoPB instance;
    instance.set_instance_id(mock_instance);
    ClusterPB c1;
    c1.set_cluster_name(mock_cluster);
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
        meta_service->get_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                                    nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
}

TEST(MetaServiceTest, GetTabletStatsTest) {
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(txn_kv.get(), nullptr);

    auto rs = std::make_shared<MockResourceManager>(txn_kv);
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs);

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
        meta_service->get_tablet_stats(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                                    nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

}

TEST(MetaServiceTest, BeginTxnTest) {
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(txn_kv.get(), nullptr);

    auto rs = std::make_shared<MockResourceManager>(txn_kv);
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs);

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
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                                    nullptr);
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
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                                    nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                                    nullptr);
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
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                                    nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                                    nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_DUPLICATED_REQ);
    }

}

TEST(MetaServiceTest, PreCommitTxnTest) {
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(txn_kv.get(), nullptr);

    auto rs = std::make_shared<MockResourceManager>(txn_kv);
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs);

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
        meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                                    nullptr);
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
        meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                                    nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_ALREADY_ABORTED);

        // TXN_STATUS_VISIBLE
        txn_info.set_status(TxnStatusPB::TXN_STATUS_VISIBLE);
        txn_inf_val.clear();
        txn_info.SerializeToString(&txn_inf_val);
        txn->put(txn_inf_key, txn_inf_val);
        ASSERT_EQ(ret = txn->commit(), 0);
        meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                                    nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_ALREADY_VISIBLE);

        // TXN_STATUS_PRECOMMITTED
        txn_info.set_status(TxnStatusPB::TXN_STATUS_PRECOMMITTED);
        txn_inf_val.clear();
        txn_info.SerializeToString(&txn_inf_val);
        txn->put(txn_inf_key, txn_inf_val);
        ASSERT_EQ(ret = txn->commit(), 0);
        meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                                    nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_ALREADY_PRECOMMITED);

    }

}

static int64_t cnt = 0;
static void create_tmp_rowset_and_meta_tablet(TxnKv* txn_kv, int64_t txn_id,
                int64_t tablet_id, int table_id = 1,
                std::string instance_id = mock_instance, int num_segments = 1) {
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
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(txn_kv.get(), nullptr);

    auto rs = std::make_shared<MockResourceManager>(txn_kv);
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs);

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
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                                        nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id = res.txn_id();
        }

        // mock rowset and tablet
        int64_t tablet_id_base = 1103;
        for (int i = 0; i < 5; ++i) {
            create_tmp_rowset_and_meta_tablet(txn_kv.get(), txn_id, tablet_id_base + i);
        }

        // precommit txn
        {
            brpc::Controller cntl;
            PrecommitTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(666);
            req.set_txn_id(txn_id);
            PrecommitTxnResponse res;
            meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                                        nullptr);
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
            meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                                        nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }
    }
}
