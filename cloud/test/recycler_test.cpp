
#include "recycler/recycler.h"

#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/selectdb_cloud.pb.h>
#include <gtest/gtest.h>

#include <string>

#include "common/config.h"
#include "common/logging.h"
#include "common/util.h"
#include "meta-service/keys.h"

static const std::string instance_id = "instance_id_recycle_test";
static constexpr int64_t table_id = 10086;
static int64_t current_time = 0;
static int64_t cnt = 0;

int main(int argc, char** argv) {
    auto conf_file = "meta_service.conf";
    if (!selectdb::config::init(conf_file, true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }
    if (!selectdb::init_glog("meta_service")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }

    using namespace std::chrono;
    current_time = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace selectdb {

static int create_prepared_rowset(FdbTxnKv* txn_kv, S3Accessor* accessor,
                                  const std::string& s3_prefix, int64_t tablet_id,
                                  int num_segments = 1) {
    ++cnt;

    std::string key;
    std::string val;

    char rowset_id[50];
    snprintf(rowset_id, sizeof(rowset_id), "%048ld", cnt);

    RecycleRowsetKeyInfo key_info {instance_id, tablet_id, rowset_id};
    recycle_rowset_key(key_info, &key);

    RecycleRowsetPB rowset_pb;
    rowset_pb.set_obj_bucket(config::test_s3_bucket);
    rowset_pb.set_obj_prefix(fmt::format("{}/data/{}", s3_prefix, tablet_id));
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
        auto object_key = fmt::format("{}/data/{}/{}_{}.dat", s3_prefix, tablet_id, rowset_id, i);
        accessor->put_object(config::test_s3_bucket, object_key, object_key);
    }
    return 0;
}

static int create_tmp_rowset(FdbTxnKv* txn_kv, S3Accessor* accessor, const std::string& s3_prefix,
                             int64_t txn_id, int64_t tablet_id, int num_segments = 1) {
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
    rowset_pb.set_s3_bucket(config::test_s3_bucket);
    rowset_pb.set_s3_prefix(fmt::format("{}/data/{}", s3_prefix, tablet_id));
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
        auto object_key = fmt::format("{}/data/{}/{}_{}.dat", s3_prefix, tablet_id, rowset_id, i);
        accessor->put_object(config::test_s3_bucket, object_key, object_key);
    }
    return 0;
}

static int create_committed_rowset(FdbTxnKv* txn_kv, S3Accessor* accessor,
                                   const std::string& s3_prefix, int64_t tablet_id,
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
    rowset_pb.set_s3_bucket(config::test_s3_bucket);
    rowset_pb.set_s3_prefix(fmt::format("{}/data/{}", s3_prefix, tablet_id));
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
        auto object_key = fmt::format("{}/data/{}/{}_{}.dat", s3_prefix, tablet_id, rowset_id, i);
        accessor->put_object(config::test_s3_bucket, object_key, object_key);
    }
    return 0;
}

static int create_tablet(FdbTxnKv* txn_kv, S3Accessor* accessor, int64_t index_id,
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

static int create_prepared_partiton(FdbTxnKv* txn_kv, S3Accessor* accessor, int64_t partition_id,
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

static int create_prepared_index(FdbTxnKv* txn_kv, S3Accessor* accessor, int64_t index_id) {
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

TEST(RecyclerTest, recycle_empty) {
    config::fdb_cluster_file_path = "fdb.cluster";
    auto txn_kv = std::make_shared<FdbTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    S3Conf s3_conf;
    s3_conf.ak = config::test_s3_ak;
    s3_conf.sk = config::test_s3_sk;
    s3_conf.endpoint = config::test_s3_endpoint;
    s3_conf.region = config::test_s3_region;
    auto accessor = std::make_shared<S3Accessor>(std::move(s3_conf));
    ASSERT_EQ(accessor->init(), 0);

    RecyclerImpl recycler(txn_kv, accessor);

    recycler.recycle_rowsets(instance_id);
}

TEST(RecyclerTest, recycle_rowsets) {
    config::rowset_retention_seconds = 0;
    config::fdb_cluster_file_path = "fdb.cluster";
    auto txn_kv = std::make_shared<FdbTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    S3Conf s3_conf;
    s3_conf.ak = config::test_s3_ak;
    s3_conf.sk = config::test_s3_sk;
    s3_conf.endpoint = config::test_s3_endpoint;
    s3_conf.region = config::test_s3_region;
    auto accessor = std::make_shared<S3Accessor>(std::move(s3_conf));
    ASSERT_EQ(accessor->init(), 0);

    RecyclerImpl recycler(txn_kv, accessor);

    for (int i = 0; i < 500; ++i) {
        create_prepared_rowset(txn_kv.get(), accessor.get(), "recycle_rowsets", 10010, 5);
    }

    recycler.recycle_rowsets(instance_id);

    // check rowset does not exist on s3
    std::vector<std::string> existed_segments;
    ASSERT_EQ(0, accessor->list(config::test_s3_bucket, "recycle_rowsets/", &existed_segments));
    ASSERT_TRUE(existed_segments.empty());
}

TEST(RecyclerTest, recycle_tmp_rowsets) {
    config::rowset_retention_seconds = 0;
    config::fdb_cluster_file_path = "fdb.cluster";
    auto txn_kv = std::make_shared<FdbTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    S3Conf s3_conf;
    s3_conf.ak = config::test_s3_ak;
    s3_conf.sk = config::test_s3_sk;
    s3_conf.endpoint = config::test_s3_endpoint;
    s3_conf.region = config::test_s3_region;
    auto accessor = std::make_shared<S3Accessor>(std::move(s3_conf));
    ASSERT_EQ(accessor->init(), 0);

    RecyclerImpl recycler(txn_kv, accessor);

    int64_t txn_id_base = 114115;
    int64_t tablet_id_base = 10015;
    for (int i = 0; i < 100; ++i) {
        for (int j = 0; j < 5; ++j) {
            create_tmp_rowset(txn_kv.get(), accessor.get(), "recycle_tmp_rowsets", txn_id_base + i,
                              tablet_id_base + j, 5);
        }
    }

    recycler.recycle_tmp_rowsets(instance_id);

    // check rowset does not exist on s3
    std::vector<std::string> existed_segments;
    ASSERT_EQ(0, accessor->list(config::test_s3_bucket, "recycle_tmp_rowsets/", &existed_segments));
    ASSERT_TRUE(existed_segments.empty());
}

TEST(RecyclerTest, recycle_tablet) {
    config::fdb_cluster_file_path = "fdb.cluster";
    auto txn_kv = std::make_shared<FdbTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    S3Conf s3_conf;
    s3_conf.ak = config::test_s3_ak;
    s3_conf.sk = config::test_s3_sk;
    s3_conf.endpoint = config::test_s3_endpoint;
    s3_conf.region = config::test_s3_region;
    auto accessor = std::make_shared<S3Accessor>(std::move(s3_conf));
    ASSERT_EQ(accessor->init(), 0);

    RecyclerImpl recycler(txn_kv, accessor);

    create_tablet(txn_kv.get(), accessor.get(), 20010, 30010, 10020);
    for (int i = 0; i < 500; ++i) {
        create_prepared_rowset(txn_kv.get(), accessor.get(), "recycle_tablet", 10020);
    }
    for (int i = 0; i < 500; ++i) {
        create_committed_rowset(txn_kv.get(), accessor.get(), "recycle_tablet", 10020);
    }

    ASSERT_EQ(0, recycler.recycle_tablet(instance_id, 10020));

    // check rowset does not exist on s3
    std::vector<std::string> existed_segments;
    ASSERT_EQ(0, accessor->list(config::test_s3_bucket, "recycle_tablet/", &existed_segments));
    ASSERT_TRUE(existed_segments.empty());
}

TEST(RecyclerTest, recycle_indexes) {
    config::index_retention_seconds = 0;
    config::fdb_cluster_file_path = "fdb.cluster";
    auto txn_kv = std::make_shared<FdbTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    S3Conf s3_conf;
    s3_conf.ak = config::test_s3_ak;
    s3_conf.sk = config::test_s3_sk;
    s3_conf.endpoint = config::test_s3_endpoint;
    s3_conf.region = config::test_s3_region;
    auto accessor = std::make_shared<S3Accessor>(std::move(s3_conf));
    ASSERT_EQ(accessor->init(), 0);

    RecyclerImpl recycler(txn_kv, accessor);

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

    recycler.recycle_indexes(instance_id);

    // check rowset does not exist on s3
    std::vector<std::string> existed_segments;
    ASSERT_EQ(0, accessor->list(config::test_s3_bucket, "recycle_indexes/", &existed_segments));
    ASSERT_TRUE(existed_segments.empty());
}

TEST(RecyclerTest, recycle_partitions) {
    config::partition_retention_seconds = 0;
    config::fdb_cluster_file_path = "fdb.cluster";
    auto txn_kv = std::make_shared<FdbTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    S3Conf s3_conf;
    s3_conf.ak = config::test_s3_ak;
    s3_conf.sk = config::test_s3_sk;
    s3_conf.endpoint = config::test_s3_endpoint;
    s3_conf.region = config::test_s3_region;
    auto accessor = std::make_shared<S3Accessor>(std::move(s3_conf));
    ASSERT_EQ(accessor->init(), 0);

    RecyclerImpl recycler(txn_kv, accessor);

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

    recycler.recycle_partitions(instance_id);

    // check rowset does not exist on s3
    std::vector<std::string> existed_segments;
    ASSERT_EQ(0, accessor->list(config::test_s3_bucket, "recycle_partitions/", &existed_segments));
    ASSERT_TRUE(existed_segments.empty());
}

} // namespace selectdb
