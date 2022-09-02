#pragma once

#include <vector>

#include "io/cloud/cloud_file_cache.h"
#include "io/cloud/cloud_file_cache_fwd.h"
#include "io/cloud/cloud_file_cache_settings.h"
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

    FileCachePtr getByPath(const IFileCache::Key& key);

    FileCacheFactory() = default;
    FileCacheFactory& operator=(const FileCacheFactory&) = delete;
    FileCacheFactory(const FileCacheFactory&) = delete;

private:
    std::vector<std::unique_ptr<IFileCache>> _caches;
};

} // namespace io
} // namespace doris
