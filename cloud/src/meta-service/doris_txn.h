
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
int get_txn_id_from_fdb_ts(std::string_view fdb_vts, int64_t* txn_id) {
    if (fdb_vts.size() != 10) return 1; // Malformed version timestamp

    // fdb version timestamp is big-endian
    //           MSB               LSB
    //           0000000000000000 0000
    //           ts               seq
    // byte addr 0 1 2 3 4 5 6 7  8 9
    int64_t ver = *reinterpret_cast<const int64_t*>(fdb_vts.data());

    // static_assert(std::endian::little); // Since c++20
    // Convert big endian to little endian
    static auto to_little = [](int64_t v) {
        v = ((v & 0xffffffff00000000) >> 32) | ((v & 0x00000000ffffffff) << 32);
        v = ((v & 0xffff0000ffff0000) >> 16) | ((v & 0x0000ffff0000ffff) << 16);
        v = ((v & 0xff00ff00ff00ff00) >> 8) | ((v & 0x00ff00ff00ff00ff) << 8);
        return v;
    };
    ver = to_little(ver);

    int64_t seq = *reinterpret_cast<const int64_t*>(fdb_vts.data() + 2);
    seq = to_little(seq);
    seq &= 0x000000000000ffff; // Strip off non-seq part

    // CAUTION: DO NOT EVER TOUCH IT!!! UNLESS YOU ARE PREPARED FOR THE DOOM!!!
    // CAUTION: DO NOT EVER TOUCH IT!!! UNLESS YOU ARE PREPARED FOR THE DOOM!!!
    // CAUTION: DO NOT EVER TOUCH IT!!! UNLESS YOU ARE PREPARED FOR THE DOOM!!!
    static constexpr int SEQ_RETAIN_BITS = 10;

    if (seq >= (1L << SEQ_RETAIN_BITS)) {
        // seq exceeds the max value
        return 2;
    }

    // Squeeze seq into ver
    seq &= ((1L << SEQ_RETAIN_BITS) - 1L); // Strip off uninterested part
    ver <<= SEQ_RETAIN_BITS;
    ver |= seq;

    *txn_id = ver;
    return 0;
}

} // namespace selectdb
// vim: et tw=100 ts=4 sw=4 cc=80:
