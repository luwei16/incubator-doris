#pragma once
#include <memory>

#include "vec/common/uint128.h"
namespace doris {
namespace io {

using uint128_t = vectorized::UInt128;
static constexpr size_t REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_CACHE_SIZE = 1024 * 1024 * 1024;
static constexpr size_t REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE = 1 * 1024 * 1024;
static constexpr size_t REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_ELEMENTS = 1024 * 1024;

class IFileCache;
using FileCacheSPtr = std::shared_ptr<IFileCache>;

struct FileCacheSettings;
} // namespace io
} // namespace doris
