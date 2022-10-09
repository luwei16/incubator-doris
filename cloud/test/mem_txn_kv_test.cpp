
// clang-format off
#include <memory>
#include "common/config.h"
#include "common/util.h"
#include "meta-service/mem_txn_kv.h"
#include "meta-service/doris_txn.h"

#include "gtest/gtest.h"
#include "meta-service/txn_kv.h"
// clang-format on

std::shared_ptr<selectdb::TxnKv> fdb_txn_kv;

int main(int argc, char** argv) {
    selectdb::config::init(nullptr, true);
    selectdb::config::fdb_cluster_file_path = "fdb.cluster";
    fdb_txn_kv = std::dynamic_pointer_cast<selectdb::TxnKv>(std::make_shared<selectdb::FdbTxnKv>()); 
    if (fdb_txn_kv.get() == nullptr) {
        std::cout << "exit get FdbTxnKv error" << std::endl;
        return -1;
    }
    if (fdb_txn_kv->init() != 0) {
        std::cout << "exit inti FdbTxnKv error" << std::endl;
        return -1;
    }

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

static void put_and_get_test(std::shared_ptr<selectdb::TxnKv> txn_kv) {
    using namespace selectdb;
    std::unique_ptr<Transaction> txn;
    std::string key = "testkey1";
    std::string val = "testvalue1";
    {
        // put
        int ret = txn_kv->create_txn(&txn);
        ASSERT_EQ(ret, 0);
        txn->put(key, val);
        ASSERT_EQ(txn->commit(), 0);
        int64_t ver1 = txn->get_committed_version();

        // get
        std::string get_val;
        txn_kv->create_txn(&txn);
        ASSERT_EQ(txn->get(key, &get_val), 0);
        int64_t ver2 = txn->get_read_version();
        ASSERT_GE(ver2, ver1);
        ASSERT_EQ(val, get_val);
        std::cout << "val:" << get_val << std::endl;

        // get not exist key
        txn_kv->create_txn(&txn);
        ASSERT_EQ(txn->get("NotExistKey", &get_val), 1);
    }
}

TEST(TxnMemKvTest, PutAndGetTest) {
    using namespace selectdb;

    auto mem_txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(mem_txn_kv.get(), nullptr);

    put_and_get_test(mem_txn_kv);
    put_and_get_test(fdb_txn_kv);
}

static void range_get_test(std::shared_ptr<selectdb::TxnKv> txn_kv) {
    using namespace selectdb;
    std::unique_ptr<Transaction> txn;
    std::vector<std::pair<std::string, std::string>> put_kv = {
         std::make_pair("key1", "val1"),
         std::make_pair("key2", "val2"),
         std::make_pair("key3", "val3"),
         std::make_pair("key4", "val4"),
         std::make_pair("key5", "val5"),
    };

    // put some kvs before test
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    for (const auto &[key, val] : put_kv) {
        txn->put(key, val);
    }
    ASSERT_EQ(txn->commit(), 0);

    // normal range get
    {
        int ret = txn_kv->create_txn(&txn);
        ASSERT_EQ(ret, 0);
        std::unique_ptr<RangeGetIterator> iter;
        ASSERT_EQ(txn->get("key1", "key4", &iter), 0);
        ASSERT_EQ(iter->size(), 3);
        ASSERT_EQ(iter->more(), false);
        int i = 0;
        while (iter->has_next()) {
            auto [key, val] = iter->next();
            ASSERT_EQ(key, put_kv[i].first);
            ASSERT_EQ(val, put_kv[i].second);
            ++i;
            std::cout << "key:" << key << " val:" << val << std::endl;
        }
    }

    // range get with not exist end key
    {
        int ret = txn_kv->create_txn(&txn);
        ASSERT_EQ(ret, 0);
        std::unique_ptr<RangeGetIterator> iter;
        ret = txn->get("key2", "key6", &iter);
        ASSERT_EQ(iter->size(), 4);
        ASSERT_EQ(iter->more(), false);
        int i = 1;
        while (iter->has_next()) {
            auto [key, val] = iter->next();
            ASSERT_EQ(key, put_kv[i].first);
            ASSERT_EQ(val, put_kv[i].second);
            ++i;
            std::cout << "key:" << key << " val:" << val << std::endl;
        }
    }

    // range get with limit
    {
        int ret = txn_kv->create_txn(&txn);
        ASSERT_EQ(ret, 0);
        std::unique_ptr<RangeGetIterator> iter;
        ret = txn->get("key1", "key4", &iter, 1);
        ASSERT_EQ(iter->size(), 1);
        ASSERT_EQ(iter->more(), true);

        auto [key, val] = iter->next();
        ASSERT_EQ(key, put_kv[0].first);
        ASSERT_EQ(val, put_kv[0].second);
    }

    // range get with begin key larger than end key
    {
        txn_kv->create_txn(&txn);
        std::unique_ptr<RangeGetIterator> iter;
        txn->get("key4", "key1", &iter);
        ASSERT_EQ(iter->size(), 0);
        txn->get("key1", "key1", &iter);
        ASSERT_EQ(iter->size(), 0);
    }
}

TEST(TxnMemKvTest, RangeGetTest) {
    using namespace selectdb;

    auto mem_txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(mem_txn_kv.get(), nullptr);

   range_get_test(mem_txn_kv);
   range_get_test(fdb_txn_kv);
}

static void remove_test(std::shared_ptr<selectdb::TxnKv> txn_kv) {
    using namespace selectdb;
    std::unique_ptr<Transaction> txn;

    std::vector<std::pair<std::string, std::string>> put_kv = {
         std::make_pair("key1", "val1"),
         std::make_pair("key2", "val2"),
         std::make_pair("key3", "val3"),
         std::make_pair("key4", "val4"),
         std::make_pair("key5", "val5"),
    };

    // put some kvs before test
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    for (const auto &[key, val] : put_kv) {
        txn->put(key, val);
    }
    ASSERT_EQ(txn->commit(), 0);

    // remove single key
    {
        int ret = txn_kv->create_txn(&txn);
        ASSERT_EQ(ret, 0);
        txn->remove("key1");
        ret = txn->commit();
        ASSERT_EQ(ret, 0);
        ret = txn_kv->create_txn(&txn);
        std::string get_val;
        ret = txn->get("key1", &get_val);
        ASSERT_EQ(ret, 1);
    }

    // range remove with begin key larger than end key
    {
        int ret = txn_kv->create_txn(&txn);
        ASSERT_EQ(ret, 0);
        txn->remove("key5", "key1");
        ret = txn->commit();
        ASSERT_NE(ret, 0);

        txn_kv->create_txn(&txn);
        std::unique_ptr<RangeGetIterator> iter;
        ret = txn->get("key2", "key6", &iter);
        ASSERT_EQ(iter->size(), 4);

    }

    // range remove
    {
        int ret = txn_kv->create_txn(&txn);
        ASSERT_EQ(ret, 0);

        txn->remove("key2", "key6");
        txn->commit();
        ASSERT_EQ(ret, 0);

        txn_kv->create_txn(&txn);
        std::unique_ptr<RangeGetIterator> iter;
        ret = txn->get("key2", "key6", &iter);
        ASSERT_EQ(iter->size(), 0);
    }
}
TEST(TxnMemKvTest, RemoveTest) {
    using namespace selectdb;

    auto mem_txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(mem_txn_kv.get(), nullptr);
    remove_test(mem_txn_kv);
    remove_test(fdb_txn_kv);
}

static void atomic_set_ver_value_test(std::shared_ptr<selectdb::TxnKv> txn_kv) {
    using namespace selectdb;
    std::unique_ptr<Transaction> txn;

    // txn_kv_test.cpp
    {
        std::string key;
        std::string val;
        int ret = txn_kv->create_txn(&txn);
        ASSERT_EQ(ret, 0);
        key.push_back('\xfe');
        key.append(" unit_test_prefix ");
        key.append(" GetVersionTest ");
        txn->atomic_set_ver_value(key, "");
        ret = txn->commit();
        int64_t ver0 = txn->get_committed_version();
        ASSERT_GT(ver0, 0);

        ret = txn_kv->create_txn(&txn);
        ASSERT_EQ(ret, 0);
        ret = txn->get(key, &val);
        ASSERT_EQ(ret, 0);
        int64_t ver1 = txn->get_read_version();
        ASSERT_GE(ver1, ver0);

        int64_t ver2;
        int64_t txn_id;
        ret = get_txn_id_from_fdb_ts(val, &txn_id);
        ASSERT_EQ(ret, 0);
        ver2 = txn_id >> 10;
        std::cout << "ver0=" << ver0 << " ver1=" << ver1 << " ver2=" << ver2 << std::endl;

        // ASSERT_EQ(ver0, ver2);
    }
}

TEST(TxnMemKvTest, AtomicSetVerValueTest) {
    using namespace selectdb;
    auto mem_txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(mem_txn_kv.get(), nullptr);

    atomic_set_ver_value_test(mem_txn_kv);
    atomic_set_ver_value_test(fdb_txn_kv);
}

static void atomic_add_test(std::shared_ptr<selectdb::TxnKv> txn_kv) {
    using namespace selectdb;
    std::unique_ptr<Transaction> txn;

    int ret = txn_kv->create_txn(&txn);
    ASSERT_EQ(ret, 0);

    txn->put("counter", "1");
    ret = txn->commit();
    ASSERT_EQ(ret, 0);

    // add
    ret = txn_kv->create_txn(&txn);
    ASSERT_EQ(ret, 0);
    txn->atomic_add("counter", 10);
    ret = txn->commit();
    ASSERT_EQ(ret, 0);

    std::string val;
    ret = txn_kv->create_txn(&txn);
    ASSERT_EQ(ret, 0);
    ret = txn->get("counter", &val);
    ASSERT_EQ(ret, 0);
    int64_t val_int = *reinterpret_cast<const int64_t*>(val.data());
    std::cout << "atomic add: " << val_int << std::endl;
    ASSERT_EQ(val_int, 59); // "1" + 10 = ASCII("1") + 10 = 49 + 10 = 59

    // sub
    ret = txn_kv->create_txn(&txn);
    ASSERT_EQ(ret, 0);
    txn->atomic_add("counter", -5);
    ret = txn->commit();
    ASSERT_EQ(ret, 0);

    ret = txn_kv->create_txn(&txn);
    ASSERT_EQ(ret, 0);
    ret = txn->get("counter", &val);
    ASSERT_EQ(ret, 0);
    val_int = *reinterpret_cast<const int64_t*>(val.data());
    std::cout << "atomic sub: " << val_int << std::endl;
    ASSERT_EQ(val_int, 54);
}

TEST(TxnMemKvTest, AtomicAddTest) {
    using namespace selectdb;
    auto mem_txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(mem_txn_kv.get(), nullptr);

    atomic_add_test(mem_txn_kv);
    atomic_add_test(fdb_txn_kv);

}

// modify identical key in one transcation
static void modify_identical_key_test(std::shared_ptr<selectdb::TxnKv> txn_kv) {
    using namespace selectdb;
    std::unique_ptr<Transaction> txn;

    // put after remove
    {
        int ret = txn_kv->create_txn(&txn);
        ASSERT_EQ(ret, 0);
        txn->put("test", "1");
        txn->remove("test");
        txn->put("test", "2");
        ASSERT_EQ(txn->commit(), 0);

        std::string get_val;
        txn_kv->create_txn(&txn);
        ASSERT_EQ(txn->get("test", &get_val), 0);
        ASSERT_EQ(get_val, "2");
    }

    // remove after put
    {
        int ret = txn_kv->create_txn(&txn);
        ASSERT_EQ(ret, 0);
        txn->put("test", "1");
        txn->remove("test");
        ASSERT_EQ(txn->commit(), 0);

        std::string get_val;
        txn_kv->create_txn(&txn);
        ASSERT_EQ(txn->get("test", &get_val), 1);
    }

}

TEST(TxnMemKvTest, ModifyIdenticalKeyTest) {
    using namespace selectdb;
    auto mem_txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(mem_txn_kv.get(), nullptr);

    modify_identical_key_test(mem_txn_kv);
    modify_identical_key_test(fdb_txn_kv);
}

static void modify_snapshot_test(std::shared_ptr<selectdb::TxnKv> txn_kv) {
    using namespace selectdb;
    std::unique_ptr<Transaction> txn_1;
    std::unique_ptr<Transaction> txn_2;

    {
        std::string get_val;
        // txn_1: put <test, version1> and commit
        int ret = txn_kv->create_txn(&txn_1);
        ASSERT_EQ(ret, 0);
        txn_1->put("test", "version1");
        ASSERT_EQ(txn_1->commit(), 0);

        // txn_2: get the snapshot of database, will see <test, version1>
        txn_kv->create_txn(&txn_2);
        ASSERT_EQ(txn_2->get("test", &get_val), 0);
        ASSERT_EQ(get_val, "version1");

        // txn_1: modify <test, version1> to <test, version2> and commit
        txn_kv->create_txn(&txn_1);
        txn_1->put("test", "version2");
        ASSERT_EQ(txn_1->commit(), 0);

        // txn_2: should still see the <test, version1>
        ASSERT_EQ(txn_2->get("test", &get_val), 0);
        ASSERT_EQ(get_val, "version1");

        // txn_2: modify <test, version1> to <test, version3> but not commit,
        // txn_2 should get <test, version3>
        txn_2->put("test", "version3");
        ASSERT_EQ(txn_2->get("test", &get_val), 0);
        ASSERT_EQ(get_val, "version3");

        // txn_2: remove <test, version3> bu not commit,
        // txn_2 should not get <test, version3>
        txn_2->remove("test");
        ASSERT_EQ(txn_2->get("test", &get_val), 1);

        // txn_1: will still see <test, version2>
        txn_kv->create_txn(&txn_1);
        ASSERT_EQ(txn_1->get("test", &get_val), 0);
        ASSERT_EQ(get_val, "version2");

        // txn_2: commit all changes
        ASSERT_EQ(txn_2->commit(), 0);

        // txn_1: should not get <test, verison2>
        txn_kv->create_txn(&txn_1);
        ASSERT_EQ(txn_1->get("test", &get_val), 1);
    }

    {
        std::string get_val;

        // txn_1: put <test, version1> and commit
        int ret = txn_kv->create_txn(&txn_1);
        ASSERT_EQ(ret, 0);
        txn_1->put("test", "version1");
        ASSERT_EQ(txn_1->commit(), 0);

        // txn_2: read the key set by atomic_xxx before commit
        txn_kv->create_txn(&txn_2);
        txn_2->atomic_set_ver_value("test", "");
        ret = txn_2->get("test", &get_val);
        // can not read the unreadable key
        ASSERT_EQ(ret != 0 && ret != 1, true);
        // after read the unreadable key, can not commit
        ASSERT_NE(txn_2->commit(), 0);

        // txn_1: still see the <test verison1>
        txn_kv->create_txn(&txn_1);
        ASSERT_EQ(txn_1->get("test", &get_val), 0);
        ASSERT_EQ(get_val, "version1");
    }
}

TEST(TxnMemKvTest, ModifySnapshotTest) {
    using namespace selectdb;
    auto mem_txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(mem_txn_kv.get(), nullptr);

    modify_snapshot_test(mem_txn_kv);
    modify_snapshot_test(fdb_txn_kv);
}
// vim: et tw=100 ts=4 sw=4 cc=80:
