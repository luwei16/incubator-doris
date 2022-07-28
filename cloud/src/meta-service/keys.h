
#pragma once
// clang-format off
#include <string>
#include <tuple>
#include <variant>
#include <vector>
// clang-format on

// clang-format off
// Possible key encoding schemas:
//
// 0x01 "instance" ${instance_id} -> InstanceInfoPB
// 
// 0x01 "txn" ${instance_id} "txn_index" ${db_id} ${label} -> set<${version_timestamp}> // version_timestamp is auto generated
// 0x01 "txn" ${instance_id} "txn_info" ${db_id} ${txn_id} -> TxnInfoPB           // txn_id is converted fomr version_timestamp
// 0x01 "txn" ${instance_id} "txn_db_tbl" ${txn_id} -> ${db_id} ${tbl_id}
// 0x01 "txn" ${instance_id} "txn_running" ${db_id} ${txn_id} -> ${table_id_list} // creaet at begin, delete at commit
//
// 0x01 "version" ${instance_id} "version_id" ${db_id} ${tbl_id} ${partition_id} -> ${version}
// 
// 0x01 "meta" ${instance_id} "rowset" ${tablet_id} ${version} -> RowsetMetaPB
// 0x01 "meta" ${instance_id} "rowset_tmp" ${txn_id} ${rowset_id} -> RowsetMetaPB
// 0x01 "meta" ${instance_id} "tablet" ${table_id} ${tablet_id} -> TabletMetaPB
// 0x01 "meta" ${instance_id} "tablet_table" ${tablet_id} -> ${table_id}
// 0x01 "meta" ${instance_id} "tablet_tmp" ${table_id} ${tablet_id} -> TabletMetaPB
// 
// 0x01 "trash" ${instacne_id} "table" -> TableTrashPB
// 
// 0x01 "node_status" ${instance_id} "compute" ${backend_id} -> ComputeNodeStatusPB
// clang-format on

namespace selectdb {

static const constexpr unsigned char CLOUD_KEY_SPACE01 = 0x01;

// clang-format off
//                                     0:instance_id
using InstanceKeyInfo      = std::tuple<std::string>;

//                                     0:instance_id  1:db_id  2:label
using TxnIndexKeyInfo      = std::tuple<std::string,  int64_t, std::string>;

//                                     0:instance_id  1:db_id  2:txn_id
using TxnInfoKeyInfo       = std::tuple<std::string,  int64_t, int64_t>;

//                                     0:instance_id  1:txn_id
using TxnDbTblKeyInfo      = std::tuple<std::string,  int64_t>;

//                                     0:instance_id  1:db_id  2:txn_id
using TxnRunningKeyInfo    = std::tuple<std::string,  int64_t, int64_t>;

//                                     0:instance_id  1:db_id  2:tbl_id  3:partition_id
using VersionKeyInfo       = std::tuple<std::string,  int64_t, int64_t,  int64_t>;

//                                     0:instance_id  1:tablet_id  2:version
using MetaRowsetKeyInfo    = std::tuple<std::string,  int64_t,     int64_t>;

//                                     0:instance_id  1:txn_id  3:rowset_id
using MetaRowsetTmpKeyInfo = std::tuple<std::string,  int64_t,  std::string>;

//                                     0:instance_id  1:table_id  2:tablet_id
using MetaTabletKeyInfo    = std::tuple<std::string,  int64_t,    int64_t>;

//                                     0:instance_id  1:tablet_id
using MetaTabletTblKeyInfo = std::tuple<std::string,  int64_t>;

//                                     0:instance_id  1:table_id  2:tablet_id
using MetaTabletTmpKeyInfo = std::tuple<std::string,  int64_t,    int64_t>;
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
void meta_tablet_table_key(const MetaTabletTblKeyInfo& in, std::string* out);
void meta_tablet_tmp_key(const MetaTabletTmpKeyInfo& in, std::string* out);

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
