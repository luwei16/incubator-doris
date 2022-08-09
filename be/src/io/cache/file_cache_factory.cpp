// clang-format off
#include "io/cache/file_cache_factory.h"

#include "io/cache/cloud_file_cache.h"
#include "io/cache/lru_file_cache.h"
#include "common/config.h"

#include <cstddef>

// clang-format on
namespace doris {
namespace io {

FileCacheFactory& FileCacheFactory::instance() {
    static FileCacheFactory ret;
    return ret;
}

void FileCacheFactory::create_file_cache(const std::string& cache_base_path,
                                         const FileCacheSettings& file_cache_settings) {
    std::shared_ptr<LRUFileCache> _cache =
            std::make_shared<LRUFileCache>(cache_base_path, file_cache_settings);
    if (config::clear_file_cache) {
        _cache->remove_if_releasable();
    }

    _caches.push_back(std::move(_cache));
}

FileCacheSPtr FileCacheFactory::getByPath(const IFileCache::Key& key) {
    return _caches[std::hash<doris::io::IFileCache::Key>()(key) % _caches.size()];
}

} // namespace io
} // namespace doris
