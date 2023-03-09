#pragma once
#include <memory>

#include "vec/common/uint128.h"

static constexpr size_t GB = 1 * 1024 * 1024 * 1024;
static constexpr size_t KB = 1024;
namespace doris {
namespace io {

using uint128_t = vectorized::UInt128;
using UInt128Hash = vectorized::UInt128Hash;
static constexpr size_t FILE_CACHE_QUEUE_DEFAULT_ELEMENTS = 100 * 1024;

struct FileCacheSettings;
// default 1 : 17 : 2
enum CacheType {
    INDEX,
    NORMAL,
    DISPOSABLE,
    TTL,
};

struct Key {
    uint128_t key;
    std::string to_string() const;

    Key() = default;
    explicit Key(const uint128_t& key_) : key(key_) {}

    bool operator==(const Key& other) const { return key == other.key; }
};

} // namespace io
} // namespace doris
