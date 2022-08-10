
// clang-format off
#include "common/config.h"
#include "common/util.h"
#include "meta-service/txn_kv.h"
#include "meta-service/doris_txn.h"

#include "gtest/gtest.h"
// clang-format on

int main(int argc, char** argv) {
    selectdb::config::init(nullptr, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}


TEST(TxnKvTest, GetVersionTest) {
    using namespace selectdb;
    config::fdb_cluster_file_path = "fdb.cluster";
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<FdbTxnKv>());
    ASSERT_NE(txn_kv.get(), nullptr);
    int ret = txn_kv->init();
    ASSERT_EQ(ret, 0);

    std::unique_ptr<Transaction> txn;
    std::string key;
    std::string val;

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

// vim: et tw=100 ts=4 sw=4 cc=80:
