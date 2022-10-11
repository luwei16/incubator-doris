
// clang-format off
#include "common/config.h"
#include "common/util.h"
#include "meta-service/txn_kv.h"
#include "meta-service/doris_txn.h"

#include "gtest/gtest.h"
// clang-format on

using namespace selectdb;

std::shared_ptr<TxnKv> txn_kv;

void init_txn_kv() {
    config::fdb_cluster_file_path = "fdb.cluster";
    txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<FdbTxnKv>());
    ASSERT_NE(txn_kv.get(), nullptr);
    int ret = txn_kv->init();
    ASSERT_EQ(ret, 0);
}

int main(int argc, char** argv) {
    selectdb::config::init(nullptr, true);
    ::testing::InitGoogleTest(&argc, argv);
    init_txn_kv();
    return RUN_ALL_TESTS();
}


TEST(TxnKvTest, GetVersionTest) {

    std::unique_ptr<Transaction> txn;
    std::string key;
    std::string val;
    int ret;
    {
        ret = txn_kv->create_txn(&txn);
        ASSERT_EQ(ret, 0);
        key.push_back('\xfe');
        key.append(" unit_test_prefix ");
        key.append(" GetVersionTest ");
        txn->atomic_set_ver_value(key, "");
        ret = txn->commit();
        int64_t ver0 = txn->get_committed_version();
        ASSERT_GT(ver0, 0);

        int ret = txn_kv->create_txn(&txn);
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
    }
}


TEST(TxnKvTest, ConflictTest) {
    std::unique_ptr<Transaction> txn, txn1, txn2;
    std::string key = "unit_test";
    std::string val, val1, val2;
    int ret = 0;

    // Historical data
    ret = txn_kv->create_txn(&txn);
    ASSERT_EQ(ret, 0);
    txn->put("unit_test", "xxxxxxxxxxxxx");
    ret = txn->commit();
    ASSERT_EQ(ret, 0);

    // txn1 begin
    ret = txn_kv->create_txn(&txn1);
    ASSERT_EQ(ret, 0);
    ret = txn1->get(key, &val1);
    ASSERT_EQ(ret, 0);
    std::cout << "val1=" << val1 << std::endl;

    // txn2 begin
    ret = txn_kv->create_txn(&txn2);
    ASSERT_EQ(ret, 0);
    ret = txn2->get(key, &val2);
    ASSERT_EQ(ret, 0);
    std::cout << "val2=" << val2 << std::endl;

    // txn2 commit
    val2 = "zzzzzzzzzzzzzzz";
    txn2->put(key, val2);
    ret = txn2->commit();
    EXPECT_EQ(ret, 0);

    // txn1 commit, intend to fail
    val1 = "yyyyyyyyyyyyyyy";
    txn1->put(key, val1);
    ret = txn1->commit();
    EXPECT_NE(ret, 0);

    ret = txn_kv->create_txn(&txn);
    ASSERT_EQ(ret, 0);
    ret = txn->get(key, &val);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(val, val2); // First wins
    std::cout << "final val=" << val << std::endl;
}

// vim: et tw=100 ts=4 sw=4 cc=80:
