
// clang-format off
#include "keys.h"

#include "codec.h"

#include <cassert>
#include <type_traits>
#include <variant>
// clang-format on

namespace selectdb {

// clang-format off
// Prefix
[[maybe_unused]] static const char* INSTANCE_KEY_PREFIX = "instance";

[[maybe_unused]] static const char* TXN_KEY_PREFIX      = "txn";
[[maybe_unused]] static const char* VERSION_KEY_PREFIX  = "version";
[[maybe_unused]] static const char* META_KEY_PREFIX     = "meta";
[[maybe_unused]] static const char* RECYCLE_KEY_PREFIX  = "recycle";
[[maybe_unused]] static const char* STATS_KEY_PREFIX    = "stats";
[[maybe_unused]] static const char* JOB_KEY_PREFIX      = "job";

// Infix
[[maybe_unused]] static const char* TXN_KEY_INFIX_INDEX       = "txn_index";
[[maybe_unused]] static const char* TXN_KEY_INFIX_INFO        = "txn_info";
[[maybe_unused]] static const char* TXN_KEY_INFIX_DB_TBL      = "txn_db_tbl";
[[maybe_unused]] static const char* TXN_KEY_INFIX_RUNNING     = "txn_running";

[[maybe_unused]] static const char* VERSION_KEY_INFIX         = "version_id";

[[maybe_unused]] static const char* META_KEY_INFIX_ROWSET     = "rowset";
[[maybe_unused]] static const char* META_KEY_INFIX_ROWSET_TMP = "rowset_tmp";
[[maybe_unused]] static const char* META_KEY_INFIX_TABLET     = "tablet";
[[maybe_unused]] static const char* META_KEY_INFIX_TABLET_IDX = "tablet_index";

[[maybe_unused]] static const char* RECYCLE_KEY_INFIX_INDEX   = "index";
[[maybe_unused]] static const char* RECYCLE_KEY_INFIX_PART    = "partition";
[[maybe_unused]] static const char* RECYCLE_KEY_TXN           = "txn";

[[maybe_unused]] static const char* STATS_KEY_INFIX_TABLET    = "tablet";

[[maybe_unused]] static const char* JOB_KEY_INFIX_TABLET      = "tablet";
// clang-format on

// clang-format off
template <typename T, typename U>
constexpr static bool is_one_of() { return std::is_same_v<T, U>; }
/**
 * Checks the first type is one of the given types (type collection)
 * @param T type to check
 * @param U first type in the collection
 * @param R the rest types in the collection
 */
template <typename T, typename U, typename... R>
constexpr static typename std::enable_if_t<0 < sizeof...(R), bool> is_one_of() {
    return ((std::is_same_v<T, U>) || is_one_of<T, R...>());
}

template <typename T, typename U>
constexpr static bool not_all_types_distinct() { return std::is_same_v<T, U>; }
/**
 * Checks if there are 2 types are the same in the given type list
 */
template <typename T, typename U, typename... R>
constexpr static typename std::enable_if_t<0 < sizeof...(R), bool> 
not_all_types_distinct() {
    // The last part of this expr is a `for` loop
    return is_one_of<T, U>() || is_one_of<T, R...>() || not_all_types_distinct<U, R...>();
}

template <typename T, typename... R>
struct check_types {
    static_assert(is_one_of<T, R...>(), "Invalid key type");
    static_assert(!not_all_types_distinct<R...>(), "Type conflict, there are at least 2 types that are identical in the list.");
    static constexpr bool value = is_one_of<T, R...>() && !not_all_types_distinct<R...>();
};
template <typename T, typename... R>
inline constexpr bool check_types_v = check_types<T, R...>::value;

template <typename T>
static void encode_prefix(const T& t, std::string* key) {
    // Input type T must be one of the following, add if needed
    static_assert(check_types_v<T,
        InstanceKeyInfo,
        TxnIndexKeyInfo, TxnInfoKeyInfo, TxnDbTblKeyInfo, TxnRunningKeyInfo,
        MetaRowsetKeyInfo, MetaRowsetTmpKeyInfo, MetaTabletKeyInfo, MetaTabletIdxKeyInfo,
        VersionKeyInfo,
        RecycleIndexKeyInfo, RecyclePartKeyInfo, RecycleRowsetKeyInfo, RecycleTxnKeyInfo,
        StatsTabletKeyInfo, JobTabletKeyInfo>);

    key->push_back(CLOUD_KEY_SPACE01);
    // Prefixes for key families
    if        constexpr (std::is_same_v<T, InstanceKeyInfo>) {
        encode_bytes(INSTANCE_KEY_PREFIX, key);
    } else if constexpr (std::is_same_v<T, TxnIndexKeyInfo>
                      || std::is_same_v<T, TxnInfoKeyInfo>
                      || std::is_same_v<T, TxnDbTblKeyInfo>
                      || std::is_same_v<T, TxnRunningKeyInfo>) {
        encode_bytes(TXN_KEY_PREFIX, key);
    } else if constexpr (std::is_same_v<T, MetaRowsetKeyInfo>
                      || std::is_same_v<T, MetaRowsetTmpKeyInfo>
                      || std::is_same_v<T, MetaTabletKeyInfo>
                      || std::is_same_v<T, MetaTabletIdxKeyInfo>) {
        encode_bytes(META_KEY_PREFIX, key);
    } else if constexpr (std::is_same_v<T, VersionKeyInfo>) {
        encode_bytes(VERSION_KEY_PREFIX, key);
    } else if constexpr (std::is_same_v<T, RecycleIndexKeyInfo>
                      || std::is_same_v<T, RecyclePartKeyInfo>
                      || std::is_same_v<T, RecycleRowsetKeyInfo>
                      || std::is_same_v<T, RecycleTxnKeyInfo>) {
        encode_bytes(RECYCLE_KEY_PREFIX, key);
    } else if constexpr (std::is_same_v<T, StatsTabletKeyInfo>) {
        encode_bytes(STATS_KEY_PREFIX, key);
    } else if constexpr (std::is_same_v<T, JobTabletKeyInfo>) {
        encode_bytes(JOB_KEY_PREFIX, key);
    } else {
        std::abort(); // Impossible
    }
    encode_bytes(std::get<0>(t), key); // instance_id
}
// clang-format on

//==============================================================================
// Resource keys
//==============================================================================

void instance_key(const InstanceKeyInfo& in, std::string* out) {
    encode_prefix(in, out); // 0x01 "instance" ${instance_id}
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
}

void meta_rowset_tmp_key(const MetaRowsetTmpKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                       // 0x01 "meta" ${instance_id}
    encode_bytes(META_KEY_INFIX_ROWSET_TMP, out); // "rowset_tmp"
    encode_int64(std::get<1>(in), out);           // txn_id
    encode_int64(std::get<2>(in), out);           // tablet_id
}

void meta_tablet_key(const MetaTabletKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                   // 0x01 "meta" ${instance_id}
    encode_bytes(META_KEY_INFIX_TABLET, out); // "tablet"
    encode_int64(std::get<1>(in), out);       // table_id
    encode_int64(std::get<2>(in), out);       // index_id
    encode_int64(std::get<3>(in), out);       // partition_id
    encode_int64(std::get<4>(in), out);       // tablet_id
}

void meta_tablet_idx_key(const MetaTabletIdxKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                       // 0x01 "meta" ${instance_id}
    encode_bytes(META_KEY_INFIX_TABLET_IDX, out); // "tablet_index"
    encode_int64(std::get<1>(in), out);           // tablet_id
}

//==============================================================================
// Recycle keys
//==============================================================================

void recycle_index_key(const RecycleIndexKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                     // 0x01 "recycle" ${instance_id}
    encode_bytes(RECYCLE_KEY_INFIX_INDEX, out); // "index"
    encode_int64(std::get<1>(in), out);         // index_id
}

void recycle_partition_key(const RecyclePartKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                    // 0x01 "recycle" ${instance_id}
    encode_bytes(RECYCLE_KEY_INFIX_PART, out); // "partition"
    encode_int64(std::get<1>(in), out);        // partition_id
}

void recycle_rowset_key(const RecycleRowsetKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                   // 0x01 "recycle" ${instance_id}
    encode_bytes(META_KEY_INFIX_ROWSET, out); // "rowset"
    encode_int64(std::get<1>(in), out);       // tablet_id
    encode_bytes(std::get<2>(in), out);       // rowset_id
}

void recycle_txn_key(const RecycleTxnKeyInfo& in, std::string* out) {
    encode_prefix(in, out);             // 0x01 "txn" ${instance_id}
    encode_bytes(RECYCLE_KEY_TXN, out); // "recycle_txn"
    encode_int64(std::get<1>(in), out); // db_id
    encode_int64(std::get<2>(in), out); // txn_id
}

void stats_tablet_key(const StatsTabletKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                    // 0x01 "stats" ${instance_id}
    encode_bytes(STATS_KEY_INFIX_TABLET, out); // "tablet"
    encode_int64(std::get<1>(in), out);        // table_id
    encode_int64(std::get<2>(in), out);        // index_id
    encode_int64(std::get<3>(in), out);        // partition_id
    encode_int64(std::get<4>(in), out);        // tablet_id
}

void job_tablet_key(const JobTabletKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                  // 0x01 "job" ${instance_id}
    encode_bytes(JOB_KEY_INFIX_TABLET, out); // "tablet"
    encode_int64(std::get<1>(in), out);      // table_id
    encode_int64(std::get<2>(in), out);      // index_id
    encode_int64(std::get<3>(in), out);      // partition_id
    encode_int64(std::get<4>(in), out);      // tablet_id
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
