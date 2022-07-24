
// clang-format off
#include "keys.h"

#include "codec.h"

#include <cassert>
#include <type_traits>
#include <variant>
// clang-format on

namespace selectdb {

// clang-format off
// Possible key encoding schemas:
//
// 0x01 "instance" ${instance_id} -> InstanceInfoPB
// 
// 0x01 "txn" ${instance_id} "txn_index" ${db_id} ${label} -> TxnIndexPB ${version_timestamp}
// 0x01 "txn" ${instance_id} "txn_info" ${db_id} ${version_timestamp} -> TxnInfoPB
// 0x01 "txn" ${instance_id} "txn_db_tbl" ${version_timestamp} -> ${db_id} ${tbl_id}
// 0x01 "txn" ${instance_id} "txn_running" ${db_id} ${version_timestamp} -> ${table_id_list}
//
// 0x01 "version" ${instance_id} "version_id" ${db_id} ${tbl_id} ${partition_id} -> ${version}
// 
// 0x01 "meta" ${instance_id} "rowset" ${tablet_id} ${version} ${rowset_id} -> RowsetMetaPB
// 0x01 "meta" ${instance_id} "rowset_tmp" ${txn_id} ${rowset_id} -> RowsetMetaPB
// 0x01 "meta" ${instance_id} "tablet" ${table_id} ${tablet_id} -> TabletMetaPB
// 0x01 "meta" ${instance_id} "tablet_table" ${tablet_id} -> ${table_id}
// 0x01 "meta" ${instance_id} "tablet_tmp" ${table_id} ${tablet_id} -> TabletMetaPB
// 
// 0x01 "trash" ${instacne_id} "table" -> TableTrashPB
// 
// 0x01 "node_status" ${instance_id} "compute" ${backend_id} -> ComputeNodeStatusPB

[[maybe_unused]] static const char* TXN_KEY_PREFIX     = "txn";
[[maybe_unused]] static const char* VERSION_KEY_PREFIX = "version";
[[maybe_unused]] static const char* META_KEY_PREFIX    = "meta";
[[maybe_unused]] static const char* TRASH_KEY_PREFIX   = "trash";

[[maybe_unused]] static const char* TXN_KEY_INFIX_INDEX   = "txn_index";
[[maybe_unused]] static const char* TXN_KEY_INFIX_INFO    = "txn_info";
[[maybe_unused]] static const char* TXN_KEY_INFIX_DB_TBL  = "txn_db_tbl";
[[maybe_unused]] static const char* TXN_KEY_INFIX_RUNNING = "txn_running";

[[maybe_unused]] static const char* VERSION_KEY_INFIX = "version_id";

[[maybe_unused]] static const char* META_KEY_INFIX_ROWSET     = "rowset";
[[maybe_unused]] static const char* META_KEY_INFIX_ROWSET_TMP = "rowset_tmp";
[[maybe_unused]] static const char* META_KEY_INFIX_TABLET     = "tablet";
[[maybe_unused]] static const char* META_KEY_INFIX_TABLET_TBL = "tablet_table";
[[maybe_unused]] static const char* META_KEY_INFIX_TABLET_TMP = "tablet_tmp";

[[maybe_unused]] static const char* TRASH_KEY_INFIX_TRASH   = "trash";
// clang-format on

template <typename T>
static void encode_prefix(const T& t, std::string* key) {
    // clang-format off
    static_assert(std::is_same_v<T, TxnIndexKeyInfo     >
               || std::is_same_v<T, TxnInfoKeyInfo      >
               || std::is_same_v<T, TxnDbTblKeyInfo     >
               || std::is_same_v<T, TxnRunningKeyInfo   >
               || std::is_same_v<T, MetaRowsetKeyInfo   >
               || std::is_same_v<T, MetaRowsetTmpKeyInfo>
               || std::is_same_v<T, MetaTabletKeyInfo   >
               || std::is_same_v<T, MetaTabletTblKeyInfo>
               || std::is_same_v<T, MetaTabletTmpKeyInfo>
               || std::is_same_v<T, VersionKeyInfo      >
               , "Invalid Key Type");

    key->push_back(CLOUD_KEY_SPACE01);
    // Prefixes for key families
    if        constexpr (std::is_same_v<T, TxnIndexKeyInfo>
                      || std::is_same_v<T, TxnInfoKeyInfo>
                      || std::is_same_v<T, TxnDbTblKeyInfo>
                      || std::is_same_v<T, TxnRunningKeyInfo>) {
        encode_bytes(TXN_KEY_PREFIX, key);
    } else if constexpr (std::is_same_v<T, MetaRowsetKeyInfo>
                      || std::is_same_v<T, MetaRowsetTmpKeyInfo>
                      || std::is_same_v<T, MetaTabletKeyInfo>
                      || std::is_same_v<T, MetaTabletTblKeyInfo>
                      || std::is_same_v<T, MetaTabletTmpKeyInfo>) {
        encode_bytes(META_KEY_PREFIX, key);
    } else if constexpr (std::is_same_v<T, VersionKeyInfo>) {
        encode_bytes(VERSION_KEY_PREFIX, key);
    } else {
        std::abort(); // Impossible
    }
    // clang-format on
    encode_bytes(std::get<0>(t), key); // instance_id
}

//==============================================================================
// Transaction keys
//==============================================================================

void txn_index_key(const TxnIndexKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                 // 0x01 "txn" ${instance_id}
    encode_bytes(TXN_KEY_INFIX_INDEX, out); // "txn_index"
    encode_int64(std::get<1>(in), out);     // db_id
    encode_bytes(std::get<2>(in), out);     // label
}

void txn_info_key(const TxnInfoKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                // 0x01 "txn" ${instance_id}
    encode_bytes(TXN_KEY_INFIX_INFO, out); // "txn_info"
    encode_int64(std::get<1>(in), out);    // db_id
    encode_int64(std::get<2>(in), out);    // txn_id
}

void txn_db_tbl_key(const TxnDbTblKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                  // 0x01 "txn" ${instance_id}
    encode_bytes(TXN_KEY_INFIX_DB_TBL, out); // "txn_db_tbl"
    encode_int64(std::get<1>(in), out);      // txn_id
}

void txn_running_key(const TxnRunningKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                   // 0x01 "txn" ${instance_id}
    encode_bytes(TXN_KEY_INFIX_RUNNING, out); // "txn_running"
    encode_int64(std::get<1>(in), out);       // db_id
    encode_int64(std::get<2>(in), out);       // txn_id
}

//==============================================================================
// Version keys
//==============================================================================

void version_key(const VersionKeyInfo& in, std::string* out) {
    encode_prefix(in, out);               // 0x01 "version" ${instance_id}
    encode_bytes(VERSION_KEY_INFIX, out); // "version_id"
    encode_int64(std::get<1>(in), out);   // db_id
    encode_int64(std::get<2>(in), out);   // tbl_id
    encode_int64(std::get<3>(in), out);   // partition_id
}

//==============================================================================
// Meta keys
//==============================================================================

void meta_rowset_key(const MetaRowsetKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                   // 0x01 "meta" ${instance_id}
    encode_bytes(META_KEY_INFIX_ROWSET, out); // "rowset"
    encode_int64(std::get<1>(in), out);       // tablet_id
    encode_int64(std::get<2>(in), out);       // version
    encode_bytes(std::get<3>(in), out);       // rowset_id
}

void meta_rowset_tmp_key(const MetaRowsetTmpKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                       // 0x01 "meta" ${instance_id}
    encode_bytes(META_KEY_INFIX_ROWSET_TMP, out); // "rowset_tmp"
    encode_int64(std::get<1>(in), out);           // txn_id
    encode_bytes(std::get<2>(in), out);           // rowset_id
}

void meta_tablet_key(const MetaTabletKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                   // 0x01 "meta" ${instance_id}
    encode_bytes(META_KEY_INFIX_TABLET, out); // "tablet"
    encode_int64(std::get<1>(in), out);       // table_id
    encode_int64(std::get<2>(in), out);       // tablet_id
}

void meta_tablet_table_key(const MetaTabletTblKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                       // 0x01 "meta" ${instance_id}
    encode_bytes(META_KEY_INFIX_TABLET_TBL, out); // "tablet"
    encode_int64(std::get<1>(in), out);           // tablet_id
}

void meta_tablet_tmp_key(const MetaTabletTmpKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                       // 0x01 "meta" ${instance_id}
    encode_bytes(META_KEY_INFIX_TABLET_TMP, out); // "tablet"
    encode_int64(std::get<1>(in), out);           // table_id
    encode_int64(std::get<2>(in), out);           // tablet_id
}

//==============================================================================
// Other keys
//==============================================================================

//==============================================================================
// Decode keys
//==============================================================================
int decode_key(std::string_view* in,
               std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>>* out) {
    int pos = 0;
    int last_len = static_cast<int>(in->size());
    while (!in->empty()) {
        int ret = 0;
        auto tag = in->at(0);
        if (tag == EncodingTag::BYTES_TAG) {
            std::string str;
            ret = decode_bytes(in, &str);
            if (ret != 0) return ret;
            out->emplace_back(std::move(str), tag, pos);
        } else if (tag == EncodingTag::NEGATIVE_FIXED_INT_TAG ||
                   tag == EncodingTag::POSITIVE_FIXED_INT_TAG) {
            int64_t v;
            ret = decode_int64(in, &v);
            if (ret != 0) return ret;
            out->emplace_back(v, tag, pos);
        } else {
            return -1;
        }
        pos += last_len - in->size();
        last_len = in->size();
    }
    return 0;
}

} // namespace selectdb
// vim: et tw=100 ts=4 sw=4 cc=80:
