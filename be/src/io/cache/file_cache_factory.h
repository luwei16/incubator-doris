#pragma once

#include <vector>

#include "io/cache/cloud_file_cache.h"
#include "io/cache/file_cache_fwd.h"
#include "io/cache/file_cache_settings.h"
namespace doris {
namespace io {

/**
 * Creates a FileCache object for cache_base_path.
 */
class FileCacheFactory {
public:
    static FileCacheFactory& instance();

    void create_file_cache(const std::string& cache_base_path,
                           const FileCacheSettings& file_cache_settings);

    FileCacheSPtr getByPath(const IFileCache::Key& key);

    FileCacheFactory() = default;
    FileCacheFactory& operator=(const FileCacheFactory&) = delete;
    FileCacheFactory(const FileCacheFactory&) = delete;

private:
    std::vector<FileCacheSPtr> _caches;
};

} // namespace io
} // namespace doris
