
#include "recycler/recycler.h"

#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/selectdb_cloud.pb.h>
#include <gtest/gtest.h>

#include <string>

#include "common/config.h"
#include "common/logging.h"
#include "common/util.h"
#include "meta-service/keys.h"
#include "meta-service/mem_txn_kv.h"
#include "meta-service/meta_service.h"
#include "mock_resource_manager.h"

static const std::string instance_id = "instance_id_recycle_test";
static constexpr int64_t table_id = 10086;
static int64_t current_time = 0;
static int64_t cnt = 0;

int main(int argc, char** argv) {
    auto conf_file = "selectdb_cloud.conf";
    if (!selectdb::config::init(conf_file, true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }
    if (!selectdb::init_glog("recycler")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }

    using namespace std::chrono;
    current_time = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace selectdb {

static int create_prepared_rowset(TxnKv* txn_kv, ObjStoreAccessor* accessor,
                                  const std::string& resource_id, int64_t tablet_id,
                                  int num_segments = 1) {
    ++cnt;

    std::string key;
    std::string val;

    char rowset_id[50];
    snprintf(rowset_id, sizeof(rowset_id), "%048ld", cnt);

    RecycleRowsetKeyInfo key_info {instance_id, tablet_id, rowset_id};
    recycle_rowset_key(key_info, &key);

    RecycleRowsetPB rowset_pb;
    rowset_pb.set_tablet_id(tablet_id);
    rowset_pb.set_resource_id(resource_id);
    rowset_pb.set_creation_time(current_time);
    rowset_pb.SerializeToString(&val);

    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != 0) {
        return -1;
    }
    txn->put(key, val);
    if (txn->commit() != 0) {
        return -1;
    }

    for (int i = 0; i < num_segments; ++i) {
        auto path = fmt::format("data/{}/{}_{}.dat", tablet_id, rowset_id, i);
        accessor->put_object(path, path);
    }
    return 0;
}

static int create_tmp_rowset(TxnKv* txn_kv, ObjStoreAccessor* accessor,
                             const std::string& resource_id, int64_t txn_id, int64_t tablet_id,
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
    rowset_pb.set_num_segments(num_segments);
    rowset_pb.set_tablet_id(tablet_id);
    rowset_pb.set_resource_id(resource_id);
    rowset_pb.set_creation_time(current_time);
    rowset_pb.SerializeToString(&val);

    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != 0) {
        return -1;
    }
    txn->put(key, val);
    if (txn->commit() != 0) {
        return -1;
    }

    for (int i = 0; i < num_segments; ++i) {
        auto path = fmt::format("data/{}/{}_{}.dat", tablet_id, rowset_id, i);
        accessor->put_object(path, path);
    }
    return 0;
}

static int create_committed_rowset(TxnKv* txn_kv, ObjStoreAccessor* accessor,
                                   const std::string& resource_id, int64_t tablet_id,
                                   int num_segments = 1) {
    ++cnt;

    std::string key;
    std::string val;

    char rowset_id[50];
    snprintf(rowset_id, sizeof(rowset_id), "%048ld", cnt);

    MetaRowsetKeyInfo key_info {instance_id, tablet_id, cnt};
    meta_rowset_key(key_info, &key);

    doris::RowsetMetaPB rowset_pb;
    rowset_pb.set_rowset_id(0); // useless but required
    rowset_pb.set_rowset_id_v2(rowset_id);
    rowset_pb.set_num_segments(num_segments);
    rowset_pb.set_tablet_id(tablet_id);
    rowset_pb.set_resource_id(resource_id);
    rowset_pb.set_creation_time(current_time);
    rowset_pb.SerializeToString(&val);

    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != 0) {
        return -1;
    }
    txn->put(key, val);
    if (txn->commit() != 0) {
        return -1;
    }

    for (int i = 0; i < num_segments; ++i) {
        auto path = fmt::format("data/{}/{}_{}.dat", tablet_id, rowset_id, i);
        accessor->put_object(path, path);
    }
    return 0;
}

static int create_tablet(TxnKv* txn_kv, ObjStoreAccessor* accessor, int64_t index_id,
                         int64_t partition_id, int64_t tablet_id) {
    std::string key;
    std::string val;

    MetaTabletKeyInfo key_info {instance_id, table_id, index_id, partition_id, tablet_id};
    meta_tablet_key(key_info, &key);

    doris::TabletMetaPB tablet_pb;
    tablet_pb.set_index_id(index_id);
    tablet_pb.set_partition_id(partition_id);
    tablet_pb.set_tablet_id(tablet_id);
    tablet_pb.SerializeToString(&val);

    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != 0) {
        return -1;
    }
    txn->put(key, val);
    if (txn->commit() != 0) {
        return -1;
    }
    return 0;
}

static int create_prepared_partiton(TxnKv* txn_kv, ObjStoreAccessor* accessor, int64_t partition_id,
                                    const std::vector<int64_t>& index_ids) {
    std::string key;
    std::string val;

    RecyclePartKeyInfo key_info {instance_id, partition_id};
    recycle_partition_key(key_info, &key);

    RecyclePartitionPB partition_pb;

    partition_pb.set_table_id(table_id);
    for (auto index_id : index_ids) {
        partition_pb.add_index_id(index_id);
    }
    partition_pb.set_creation_time(current_time);
    partition_pb.SerializeToString(&val);

    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != 0) {
        return -1;
    }
    txn->put(key, val);
    if (txn->commit() != 0) {
        return -1;
    }
    return 0;
}

static int create_prepared_index(TxnKv* txn_kv, ObjStoreAccessor* accessor, int64_t index_id) {
    std::string key;
    std::string val;

    RecycleIndexKeyInfo key_info {instance_id, index_id};
    recycle_index_key(key_info, &key);

    RecycleIndexPB index_pb;

    index_pb.set_table_id(table_id);
    index_pb.set_creation_time(current_time);
    index_pb.SerializeToString(&val);

    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != 0) {
        return -1;
    }
    txn->put(key, val);
    if (txn->commit() != 0) {
        return -1;
    }
    return 0;
}

static int get_txn_info(std::shared_ptr<TxnKv> txn_kv, std::string instance_id, int64_t db_id,
                        int64_t txn_id, TxnInfoPB& txn_info_pb) {
    std::string txn_inf_key;
    std::string txn_inf_val;
    TxnInfoKeyInfo txn_inf_key_info {instance_id, db_id, txn_id};

    LOG(INFO) << instance_id << "|" << db_id << "|" << txn_id;

    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != 0) {
        return -1;
    }
    txn_info_key(txn_inf_key_info, &txn_inf_key);
    LOG(INFO) << "txn_inf_key:" << hex(txn_inf_key);
    int ret = txn->get(txn_inf_key, &txn_inf_val);
    if (ret != 0) {
        LOG(WARNING) << "txn->get failed, ret=" << ret;
        return -2;
    }

    if (!txn_info_pb.ParseFromString(txn_inf_val)) {
        LOG(WARNING) << "ParseFromString failed";
        return -3;
    }
    LOG(INFO) << "txn_info_pb" << txn_info_pb.DebugString();
    if (txn->commit() != 0) {
        LOG(WARNING) << "txn->commit failed, ret=" << ret;
        return -4;
    }
    return 0;
}

TEST(RecyclerTest, recycle_empty) {
    config::fdb_cluster_file_path = "fdb.cluster";
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    auto obj_info = instance.add_obj_info();
    obj_info->set_id("recycle_empty");
    obj_info->set_ak(config::test_s3_ak);
    obj_info->set_sk(config::test_s3_sk);
    obj_info->set_endpoint(config::test_s3_endpoint);
    obj_info->set_region(config::test_s3_region);
    obj_info->set_bucket(config::test_s3_bucket);
    obj_info->set_prefix("recycle_empty");

    InstanceRecycler recycler(txn_kv, instance);

    recycler.recycle_rowsets();
}

TEST(RecyclerTest, recycle_rowsets) {
    config::rowset_retention_seconds = 0;
    config::fdb_cluster_file_path = "fdb.cluster";
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    auto obj_info = instance.add_obj_info();
    obj_info->set_id("recycle_rowsets");
    obj_info->set_ak(config::test_s3_ak);
    obj_info->set_sk(config::test_s3_sk);
    obj_info->set_endpoint(config::test_s3_endpoint);
    obj_info->set_region(config::test_s3_region);
    obj_info->set_bucket(config::test_s3_bucket);
    obj_info->set_prefix("recycle_rowsets");

    InstanceRecycler recycler(txn_kv, instance);

    auto accessor = recycler.accessor_map_.begin()->second;
    for (int i = 0; i < 500; ++i) {
        create_prepared_rowset(txn_kv.get(), accessor.get(), "recycle_rowsets", 10010, 5);
    }

    recycler.recycle_rowsets();

    // check rowset does not exist on s3
    std::vector<std::string> existed_segments;
    ASSERT_EQ(0, accessor->list("data/", &existed_segments));
    ASSERT_TRUE(existed_segments.empty());
}

TEST(RecyclerTest, recycle_tmp_rowsets) {
    config::rowset_retention_seconds = 0;
    config::fdb_cluster_file_path = "fdb.cluster";
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    auto obj_info = instance.add_obj_info();
    obj_info->set_id("recycle_tmp_rowsets");
    obj_info->set_ak(config::test_s3_ak);
    obj_info->set_sk(config::test_s3_sk);
    obj_info->set_endpoint(config::test_s3_endpoint);
    obj_info->set_region(config::test_s3_region);
    obj_info->set_bucket(config::test_s3_bucket);
    obj_info->set_prefix("recycle_tmp_rowsets");

    InstanceRecycler recycler(txn_kv, instance);

    auto accessor = recycler.accessor_map_.begin()->second;
    int64_t txn_id_base = 114115;
    int64_t tablet_id_base = 10015;
    for (int i = 0; i < 100; ++i) {
        for (int j = 0; j < 5; ++j) {
            create_tmp_rowset(txn_kv.get(), accessor.get(), "recycle_tmp_rowsets", txn_id_base + i,
                              tablet_id_base + j, 5);
        }
    }

    recycler.recycle_tmp_rowsets();

    // check rowset does not exist on s3
    std::vector<std::string> existed_segments;
    ASSERT_EQ(0, accessor->list("data/", &existed_segments));
    for (auto& path : existed_segments) {
        std::cout << path << std::endl;
    }
    ASSERT_TRUE(existed_segments.empty());
}

TEST(RecyclerTest, recycle_tablet) {
    config::fdb_cluster_file_path = "fdb.cluster";
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    auto obj_info = instance.add_obj_info();
    obj_info->set_id("recycle_tablet");
    obj_info->set_ak(config::test_s3_ak);
    obj_info->set_sk(config::test_s3_sk);
    obj_info->set_endpoint(config::test_s3_endpoint);
    obj_info->set_region(config::test_s3_region);
    obj_info->set_bucket(config::test_s3_bucket);
    obj_info->set_prefix("recycle_tablet");

    InstanceRecycler recycler(txn_kv, instance);

    auto accessor = recycler.accessor_map_.begin()->second;
    create_tablet(txn_kv.get(), accessor.get(), 20010, 30010, 10020);
    for (int i = 0; i < 500; ++i) {
        create_prepared_rowset(txn_kv.get(), accessor.get(), "recycle_tablet", 10020);
    }
    for (int i = 0; i < 500; ++i) {
        create_committed_rowset(txn_kv.get(), accessor.get(), "recycle_tablet", 10020);
    }

    ASSERT_EQ(0, recycler.recycle_tablet(10020));

    // check rowset does not exist on s3
    std::vector<std::string> existed_segments;
    ASSERT_EQ(0, accessor->list("data/", &existed_segments));
    ASSERT_TRUE(existed_segments.empty());
}

TEST(RecyclerTest, recycle_indexes) {
    config::index_retention_seconds = 0;
    config::fdb_cluster_file_path = "fdb.cluster";
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    auto obj_info = instance.add_obj_info();
    obj_info->set_id("recycle_indexes");
    obj_info->set_ak(config::test_s3_ak);
    obj_info->set_sk(config::test_s3_sk);
    obj_info->set_endpoint(config::test_s3_endpoint);
    obj_info->set_region(config::test_s3_region);
    obj_info->set_bucket(config::test_s3_bucket);
    obj_info->set_prefix("recycle_indexes");

    InstanceRecycler recycler(txn_kv, instance);

    auto accessor = recycler.accessor_map_.begin()->second;
    create_prepared_index(txn_kv.get(), accessor.get(), 20015);
    int64_t tablet_id_base = 10100;
    for (int i = 0; i < 100; ++i) {
        int64_t tablet_id = tablet_id_base + i;
        create_tablet(txn_kv.get(), accessor.get(), 20015, 30015, tablet_id);
        for (int j = 0; j < 10; ++j) {
            create_prepared_rowset(txn_kv.get(), accessor.get(), "recycle_indexes", tablet_id);
        }
        for (int j = 0; j < 10; ++j) {
            create_committed_rowset(txn_kv.get(), accessor.get(), "recycle_indexes", tablet_id);
        }
    }

    recycler.recycle_indexes();

    // check rowset does not exist on s3
    std::vector<std::string> existed_segments;
    ASSERT_EQ(0, accessor->list("data/", &existed_segments));
    ASSERT_TRUE(existed_segments.empty());
}

TEST(RecyclerTest, recycle_partitions) {
    config::partition_retention_seconds = 0;
    config::fdb_cluster_file_path = "fdb.cluster";
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    auto obj_info = instance.add_obj_info();
    obj_info->set_id("recycle_partitions");
    obj_info->set_ak(config::test_s3_ak);
    obj_info->set_sk(config::test_s3_sk);
    obj_info->set_endpoint(config::test_s3_endpoint);
    obj_info->set_region(config::test_s3_region);
    obj_info->set_bucket(config::test_s3_bucket);
    obj_info->set_prefix("recycle_partitions");

    InstanceRecycler recycler(txn_kv, instance);

    auto accessor = recycler.accessor_map_.begin()->second;
    std::vector<int64_t> index_ids {20200, 20201, 20202, 20203, 20204};
    create_prepared_partiton(txn_kv.get(), accessor.get(), 30020, index_ids);
    int64_t tablet_id_base = 10100;
    for (auto index_id : index_ids) {
        for (int i = 0; i < 20; ++i) {
            int64_t tablet_id = tablet_id_base + i;
            create_tablet(txn_kv.get(), accessor.get(), index_id, 30020, tablet_id);
            for (int j = 0; j < 10; ++j) {
                create_prepared_rowset(txn_kv.get(), accessor.get(), "recycle_partitions",
                                       tablet_id);
            }
            for (int j = 0; j < 10; ++j) {
                create_committed_rowset(txn_kv.get(), accessor.get(), "recycle_partitions",
                                        tablet_id);
            }
        }
    }

    recycler.recycle_partitions();

    // check rowset does not exist on s3
    std::vector<std::string> existed_segments;
    ASSERT_EQ(0, accessor->list("data/", &existed_segments));
    ASSERT_TRUE(existed_segments.empty());
}

TEST(RecyclerTest, abort_timeout_txn) {
    config::stream_load_default_timeout_second = 0;
    config::fdb_cluster_file_path = "fdb.cluster";

    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(txn_kv.get(), nullptr);
    auto rs = std::make_shared<MockResourceManager>(txn_kv);
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs);
    ASSERT_EQ(txn_kv->init(), 0);

    int64_t db_id = 666;
    int64_t table_id = 1234;
    int64_t txn_id = -1;
    {
        brpc::Controller cntl;
        BeginTxnRequest req;

        req.set_cloud_unique_id("test_cloud_unique_id");
        TxnInfoPB txn_info_pb;
        txn_info_pb.set_db_id(db_id);
        txn_info_pb.set_label("abort_timeout_txn");
        txn_info_pb.add_table_ids(table_id);
        req.mutable_txn_info()->CopyFrom(txn_info_pb);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        txn_id = res.txn_id();
        ASSERT_GT(txn_id, -1);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
    InstanceInfoPB instance;
    instance.set_instance_id(mock_instance);
    InstanceRecycler recycler(txn_kv, instance);
    recycler.abort_timeout_txn();
    TxnInfoPB txn_info_pb;
    get_txn_info(txn_kv, mock_instance, db_id, txn_id, txn_info_pb);
    ASSERT_EQ(txn_info_pb.status(), TxnStatusPB::TXN_STATUS_ABORTED);
}

TEST(RecyclerTest, recycle_expired_txn_label) {
    config::label_keep_max_second = 0;
    config::stream_load_default_timeout_second = 0;
    config::fdb_cluster_file_path = "fdb.cluster";

    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(txn_kv.get(), nullptr);
    auto rs = std::make_shared<MockResourceManager>(txn_kv);
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs);
    ASSERT_EQ(txn_kv->init(), 0);

    int64_t db_id = 888;
    int64_t table_id = 1234;
    int64_t txn_id = -1;
    {
        brpc::Controller cntl;
        BeginTxnRequest req;

        req.set_cloud_unique_id("test_cloud_unique_id2");
        TxnInfoPB txn_info_pb;
        txn_info_pb.set_db_id(db_id);
        txn_info_pb.set_label("recycle_expired_txn_label");
        txn_info_pb.add_table_ids(table_id);
        req.mutable_txn_info()->CopyFrom(txn_info_pb);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        txn_id = res.txn_id();
        ASSERT_GT(txn_id, -1);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
    InstanceInfoPB instance;
    instance.set_instance_id(mock_instance);
    InstanceRecycler recycler(txn_kv, instance);
    recycler.abort_timeout_txn();
    TxnInfoPB txn_info_pb;
    get_txn_info(txn_kv, mock_instance, db_id, txn_id, txn_info_pb);
    ASSERT_EQ(txn_info_pb.status(), TxnStatusPB::TXN_STATUS_ABORTED);

    {
        brpc::Controller cntl;
        BeginTxnRequest req;

        req.set_cloud_unique_id("test_cloud_unique_id3");
        TxnInfoPB txn_info_pb;
        txn_info_pb.set_db_id(db_id);
        txn_info_pb.set_label("recycle_expired_txn_label");
        txn_info_pb.add_table_ids(table_id);
        req.mutable_txn_info()->CopyFrom(txn_info_pb);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        txn_id = res.txn_id();
        ASSERT_GT(txn_id, -1);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
}

} // namespace selectdb
