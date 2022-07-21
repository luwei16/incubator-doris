
// clang-format off
#include "meta-service/keys.h"

#include <gtest/gtest.h>

#include <cstring>
#include <iostream>
#include <random>
// clang-format on

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

// FIXME: remove duplicated code
static std::string hex(std::string_view str) {
    std::stringstream ss;
    for (auto& i : str) {
        ss << std::hex << std::setw(2) << std::setfill('0') << ((int16_t)i & 0xff);
    }
    return ss.str();
}

// extern
namespace selectdb {
void encode_int64(int64_t val, std::string* b);
int decode_int64(std::string_view* in, int64_t* val);
void encode_bytes(std::string_view bytes, std::string* b);
int decode_bytes(std::string_view* in, std::string* out);
} // namespace selectdb

// clang-format off
// Possible key encoding schemas:
//
// 0x01 "instance" ${instance_id} -> InstanceInfoPB
// 
// 0x01 "txn" ${instance_id} "txn_index" ${db_id} ${label} -> TxnIndexPB ${version_timestamp}
// 0x01 "txn" ${instance_id} "txn_info" ${db_id} ${version_timestamp} -> TxnInfoPB
// 0x01 "txn" ${instance_id} "txn_db_tbl" ${version_timestamp} -> ${db_id} ${tbl_id}
// 0x01 "txn" ${instance_id} "txn_running" ${db_id} ${version_timestamp} -> ${table_id_list} // creaet at begin, delete at commit
//
// 0x01 "version" ${instance_id} "version_id" ${db_id} ${tbl_id} ${partition_id} -> ${version}
// 
// 0x01 "meta" ${instance_id} "rowset" ${tablet_id} ${version} ${rowset_id} -> RowsetMetaPB
// 0x01 "meta" ${instance_id} "rowset_tmp" ${txn_id} ${rowset_id} -> RowsetMetaPB
// 0x01 "meta" ${instance_id} "tablet" ${table_id} ${tablet_id} -> TabletMetaPB
// 
// 0x01 "trash" ${instacne_id} "table" -> TableTrashPB
// 
// 0x01 "node_status" ${instance_id} "compute" ${backend_id} -> ComputeNodeStatusPB
// clang-format on

TEST(KeysTest, KeysTest) {
    using namespace selectdb;
    std::string instance_id = "instance_id_deadbeef";

    // 0x01 "meta" ${instance_id} "rowset" ${tablet_id} ${version} ${rowset_id}
    {
        int64_t tablet_id = 10086;
        int64_t version = 100;
        int64_t rowset_id = 10010;
        MetaRowsetKeyInfo rowset_key {instance_id, tablet_id, version, rowset_id};
        std::string encoded_rowset_key0;
        meta_rowset_key(rowset_key, &encoded_rowset_key0);
        std::cout << hex(encoded_rowset_key0) << std::endl;

        std::string dec_instance_id;
        int64_t dec_tablet_id = 0;
        int64_t dec_version = 0;
        int64_t dec_rowset_id = 0;

        std::string_view key_sv(encoded_rowset_key0);
        std::string dec_meta_prefix;
        std::string dec_rowset_prefix;
        key_sv.remove_prefix(1); // Remove CLOUD_KEY_SPACE01
        ASSERT_EQ(decode_bytes(&key_sv, &dec_meta_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_rowset_prefix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_tablet_id), 0) << hex(key_sv);
        ASSERT_EQ(decode_int64(&key_sv, &dec_version), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_rowset_id), 0);

        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(tablet_id, dec_tablet_id);
        EXPECT_EQ(version, dec_version);
        EXPECT_EQ(rowset_id, dec_rowset_id);

        std::get<2>(rowset_key) = version + 1;
        std::string encoded_rowset_key1;
        meta_rowset_key(rowset_key, &encoded_rowset_key1);
        std::cout << hex(encoded_rowset_key1) << std::endl;

        ASSERT_GT(encoded_rowset_key1, encoded_rowset_key0);
    }

    // MetaTabletKeyInfo tablet_key;
}

// vim: et tw=100 ts=4 sw=4 cc=80:
