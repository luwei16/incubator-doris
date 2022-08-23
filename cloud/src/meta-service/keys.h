
#pragma once
// clang-format off
#include <cstdint>
#include <string>
#include <tuple>
#include <variant>
#include <vector>
// clang-format on

// clang-format off
// Key encoding schemes:
//
// 0x01 "instance" ${instance_id}                                                            -> InstanceInfoPB
//
// 0x01 "txn" ${instance_id} "txn_index" ${db_id} ${label}                                   -> TxnInfoPB ${version_timestamp}
// 0x01 "txn" ${instance_id} "txn_info" ${db_id} ${txn_id}                                   -> TxnInfoPB
// 0x01 "txn" ${instance_id} "txn_db_tbl" ${txn_id}                                          -> ${db_id} ${tbl_id}
// 0x01 "txn" ${instance_id} "txn_running" ${db_id} ${txn_id}                                -> ${table_id_list}
//
// 0x01 "version" ${instance_id} "version_id" ${db_id} ${tbl_id} ${partition_id}             -> ${version}
//
// 0x01 "meta" ${instance_id} "rowset" ${tablet_id} ${version}                               -> RowsetMetaPB
// 0x01 "meta" ${instance_id} "rowset_tmp" ${txn_id} ${rowset_id}                            -> RowsetMetaPB
// 0x01 "meta" ${instance_id} "tablet" ${table_id} ${index_id} ${partition_id} ${tablet_id}  -> TabletMetaPB
// 0x01 "meta" ${instance_id} "tablet_index" ${tablet_id}                                    -> TabletIndexPB
//
// 0x01 "stats" ${instance_id} "tablet" ${table_id} ${index_id} ${partition_id} ${tablet_id} -> TabletStatsPB
//
// 0x01 "recycle" ${instance_id} "index" ${index_id}                                         -> RecycleIndexPB
// 0x01 "recycle" ${instance_id} "partition" ${partition_id}                                 -> RecyclePartitionPB
// 0x01 "recycle" ${instance_id} "rowset" ${tablet_id} ${rowset_id}                          -> RecycleRowsetPB
// clang-format on

namespace selectdb {

static const constexpr unsigned char CLOUD_KEY_SPACE01 = 0x01;

// clang-format off
/**
 * Wraps std::tuple for differnet types even if the underlying type is the same.
 * 
 * @param N for elemination of same underlying types of type alias when we use
 *          `using` to declare a new type.
 *
 * @param Base for base tuple, the underlying type
 */
template<size_t N, typename Base>
struct BasicKeyInfo : Base {
    template<typename... Args>
    BasicKeyInfo(Args&&... args) : Base(std::forward<Args>(args)...) {}
};

// ATTN: newly added key must have different type number

//                                                      0:instance_id
using InstanceKeyInfo      = BasicKeyInfo<0 , std::tuple<std::string>>;

//                                                      0:instance_id  1:db_id  2:label
using TxnIndexKeyInfo      = BasicKeyInfo<1 , std::tuple<std::string,  int64_t, std::string>>;

//                                                      0:instance_id  1:db_id  2:txn_id
using TxnInfoKeyInfo       = BasicKeyInfo<2 , std::tuple<std::string,  int64_t, int64_t>>;

//                                                      0:instance_id  1:txn_id
using TxnDbTblKeyInfo      = BasicKeyInfo<3 , std::tuple<std::string,  int64_t>>;

//                                                      0:instance_id  1:db_id  2:txn_id
using TxnRunningKeyInfo    = BasicKeyInfo<5 , std::tuple<std::string,  int64_t, int64_t>>;

//                                                      0:instance_id  1:db_id  2:tbl_id  3:partition_id
using VersionKeyInfo       = BasicKeyInfo<6 , std::tuple<std::string,  int64_t, int64_t,  int64_t>>;

//                                                      0:instance_id  1:tablet_id  2:version
using MetaRowsetKeyInfo    = BasicKeyInfo<7 , std::tuple<std::string,  int64_t,     int64_t>>;

//                                                      0:instance_id  1:txn_id  2:tablet_id
using MetaRowsetTmpKeyInfo = BasicKeyInfo<8 , std::tuple<std::string,  int64_t,  int64_t>>;

//                                                      0:instance_id  1:table_id  2:index_id  3:part_id  4:tablet_id
using MetaTabletKeyInfo    = BasicKeyInfo<9 , std::tuple<std::string,  int64_t,    int64_t,    int64_t,   int64_t>>;

//                                                      0:instance_id  1:tablet_id
using MetaTabletIdxKeyInfo = BasicKeyInfo<10, std::tuple<std::string,  int64_t>>;

//                                                      0:instance_id  1:index_id
using RecycleIndexKeyInfo  = BasicKeyInfo<11, std::tuple<std::string,  int64_t>>;

//                                                      0:instance_id  1:part_id
using RecyclePartKeyInfo   = BasicKeyInfo<12, std::tuple<std::string,  int64_t>>;

//                                                      0:instance_id  1:tablet_id  2:rowset_id
using RecycleRowsetKeyInfo = BasicKeyInfo<13, std::tuple<std::string,  int64_t,     std::string>>;

//                                                      0:instance_id  1:table_id  2:index_id  3:part_id  4:tablet_id
using StatsTabletKeyInfo   = BasicKeyInfo<14 , std::tuple<std::string,  int64_t,    int64_t,    int64_t,   int64_t>>;
// clang-format on

void instance_key(const InstanceKeyInfo& in, std::string* out);

void txn_index_key(const TxnIndexKeyInfo& in, std::string* out);
void txn_info_key(const TxnInfoKeyInfo& in, std::string* out);
void txn_db_tbl_key(const TxnDbTblKeyInfo& in, std::string* out);
void txn_running_key(const TxnRunningKeyInfo& in, std::string* out);

void version_key(const VersionKeyInfo& in, std::string* out);

void meta_rowset_key(const MetaRowsetKeyInfo& in, std::string* out);
void meta_rowset_tmp_key(const MetaRowsetTmpKeyInfo& in, std::string* out);
void meta_tablet_key(const MetaTabletKeyInfo& in, std::string* out);
void meta_tablet_idx_key(const MetaTabletIdxKeyInfo& in, std::string* out);

void recycle_index_key(const RecycleIndexKeyInfo& in, std::string* out);
void recycle_partition_key(const RecyclePartKeyInfo& in, std::string* out);
void recycle_rowset_key(const RecycleRowsetKeyInfo& in, std::string* out);

void stats_tablet_key(const StatsTabletKeyInfo& in, std::string* out);
// TODO: add a family of decoding functions if needed

/**
 * Deocdes a given key without key space byte (the first byte).
 * Note that the input may be partially decode if the return value is non-zero.
 *
 * @param in input byte stream, successfully decoded part will be consumed 
 * @param out the vector of each <field decoded, field type and its position> in the input stream
 * @return 0 for successful decoding of the entire input, otherwise error.
 */
int decode_key(std::string_view* in,
               std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>>* out);

} // namespace selectdb
// vim: et tw=100 ts=4 sw=4 cc=80:
