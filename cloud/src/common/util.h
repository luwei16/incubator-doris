
#pragma once

#include <string>

namespace google::protobuf {
class Message;
}

namespace selectdb {

std::string hex(std::string_view str);

std::string unhex(std::string_view str);

/**
 * Prettifies the given key, the first byte must be key space tag, say 0x01, and
 * the remaining part must be the output of codec funtion family.
 *
 * The result is like following:
 *
 * /------------------------------------------------------------------------------------------------------- 0. key space: 1
 * | /----------------------------------------------------------------------------------------------------- 1. txn
 * | |           /----------------------------------------------------------------------------------------- 2. instance_id_deadbeef
 * | |           |                                             /------------------------------------------- 3. txn_label
 * | |           |                                             |                       /------------------- 4. 10003
 * | |           |                                             |                       |                 /- 5. insert_3fd5ad12d0054a9b-8c776d3218c6adb7
 * | |           |                                             |                       |                 |
 * v v           v                                             v                       v                 v
 * 011074786e000110696e7374616e63655f69645f646561646265656600011074786e5f696e646578000112000000000000271310696e736572745f336664356164313264303035346139622d386337373664333231386336616462370001
 *
 * @param key_hex encoded key hex string
 * @param unicode whether to use unicode (UTF8) to draw line, default false.
 * @return the pretty format, empty result if error occurs
 */
std::string prettify_key(std::string_view key_hex, bool unicode = false);

/**
 * Converts proto message to json string
 *
 * @return empty string if conversion failed
 */
std::string proto_to_json(const ::google::protobuf::Message& msg, bool add_whitespace = false);

} // namespace selectdb

// vim: et tw=100 ts=4 sw=4 cc=80 nowrap:
