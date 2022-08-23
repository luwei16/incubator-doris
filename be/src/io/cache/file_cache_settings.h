#pragma once

#include "io/cache/file_cache_fwd.h"

namespace doris {
namespace io {

struct FileCacheSettings {
    size_t max_size = 0;
    size_t max_elements = REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_ELEMENTS;
    size_t max_file_segment_size = REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE;
};

} // namespace io
} // namespace doris
