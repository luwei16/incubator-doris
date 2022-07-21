
#pragma once
// clang-format off
#include <string>
#include <tuple>
// clang-format on

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

namespace selectdb {

static const constexpr unsigned char CLOUD_KEY_SPACE01 = 0x01;

// clang-format off
//                                     0:instance_id  1:db_id  2:label
using TxnIndexKeyInfo      = std::tuple<std::string,  int64_t, std::string>;

//                                     0:instance_id  1:db_id
using TxnInfoKeyInfo       = std::tuple<std::string,  int64_t>;

//                                     0:instance_id
using TxnDbTblKeyInfo      = std::tuple<std::string>;

//                                     0:instance_id  1:db_id
using TxnRunningKeyInfo    = std::tuple<std::string,  int64_t>;

//                                     0:instance_id  1:db_id  2:tbl_id  3:partition_id
using VersionKeyInfo       = std::tuple<std::string,  int64_t, int64_t,  int64_t>;

//                                     0:instance_id  1:tablet_id  2:version  3:rowset_id
using MetaRowsetKeyInfo    = std::tuple<std::string,  int64_t,     int64_t,   int64_t>;

//                                     0:instance_id  1:txn_id  3:rowset_id
using MetaRowsetTmpKeyInfo = std::tuple<std::string,  int64_t,  int64_t>;

//                                     0:instance_id  1:table_id  2:tablet_id
using MetaTabletKeyInfo    = std::tuple<std::string,  int64_t,    int64_t>;

//                                     0:instance_id  1:table_id  2:tablet_id
using MetaTabletTmpKeyInfo = std::tuple<std::string,  int64_t,    int64_t>;
// clang-format on

void txn_index_key(const TxnIndexKeyInfo& in, std::string* out);
void txn_info_key(const TxnInfoKeyInfo& in, std::string* out);
void txn_db_tbl_key(const TxnDbTblKeyInfo& in, std::string* out);
void txn_running_key(const TxnRunningKeyInfo& in, std::string* out);

void version_key(const VersionKeyInfo& in, std::string* out);

void meta_rowset_key(const MetaRowsetKeyInfo& in, std::string* out);
void meta_rowset_tmp_key(const MetaRowsetTmpKeyInfo& in, std::string* out);
void meta_tablet_key(const MetaTabletKeyInfo& in, std::string* out);
void meta_tablet_tmp_key(const MetaTabletTmpKeyInfo& in, std::string* out);

// TODO: add a family of decoding functions if needed

} // namespace selectdb
// vim: et tw=100 ts=4 sw=4 cc=80:
