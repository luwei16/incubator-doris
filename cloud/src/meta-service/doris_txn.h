
#pragma once

// clang-format off
#include <string>
// clang-format on

namespace selectdb {

/**
 * Converts 10-byte fdb version timestamp to 8-byte doris txn id
 * 
 * @param fdb_vts 10 bytes fdb version timestamp
 * @param txn_id 8-byte output txn_id for doris
 * @return 0 for success otherwise error
 */
int get_txn_id_from_fdb_ts(std::string_view fdb_vts, int64_t* txn_id);

} // namespace selectdb
// vim: et tw=100 ts=4 sw=4 cc=80:
