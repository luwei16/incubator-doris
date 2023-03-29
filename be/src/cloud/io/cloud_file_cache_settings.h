#pragma once

#include "cloud/io/cloud_file_cache_fwd.h"
#include "common/config.h"

namespace doris {
namespace io {
// [query:disposable:index:total]
constexpr std::array<size_t, 4> percentage {17, 2, 1, 20};
static_assert(percentage[0] + percentage[1] + percentage[2] == percentage[3]);
struct FileCacheSettings {
    size_t total_size {0};
    size_t disposable_queue_size {0};
    size_t disposable_queue_elements {0};
    size_t index_queue_size {0};
    size_t index_queue_elements {0};
    size_t query_queue_size {0};
    size_t query_queue_elements {0};
    size_t max_file_segment_size {0};
    size_t max_query_cache_size {0};
};

inline FileCacheSettings calc_settings(size_t total_size, size_t max_query_cache_size) {
    io::FileCacheSettings settings;
    settings.total_size = total_size;
    settings.max_file_segment_size = config::file_cache_max_file_segment_size;
    settings.max_query_cache_size = max_query_cache_size;
    size_t per_size = settings.total_size / io::percentage[3];
    settings.disposable_queue_size = per_size * io::percentage[1];
    settings.disposable_queue_elements =
            std::max(settings.disposable_queue_size / settings.max_file_segment_size,
                     io::FILE_CACHE_QUEUE_DEFAULT_ELEMENTS);

    settings.index_queue_size = per_size * io::percentage[2];
    settings.index_queue_elements =
            std::max(settings.index_queue_size / settings.max_file_segment_size,
                     io::FILE_CACHE_QUEUE_DEFAULT_ELEMENTS);

    settings.query_queue_size =
            settings.total_size - settings.disposable_queue_size - settings.index_queue_size;
    settings.query_queue_elements =
            std::max(settings.query_queue_size / settings.max_file_segment_size,
                     io::FILE_CACHE_QUEUE_DEFAULT_ELEMENTS);
    return settings;
}
} // namespace io
} // namespace doris
