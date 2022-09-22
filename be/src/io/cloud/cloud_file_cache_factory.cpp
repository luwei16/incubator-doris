// clang-format off
#include "io/cloud/cloud_file_cache_factory.h"

#include "io/cloud/cloud_file_cache.h"
#include "io/cloud/cloud_lru_file_cache.h"
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
    std::unique_ptr<IFileCache> cache =
            std::make_unique<LRUFileCache>(cache_base_path, file_cache_settings);
    if (config::clear_file_cache) {
        cache->remove_if_releasable(true);
        cache->remove_if_releasable(false);
    }

    _caches.push_back(std::move(cache));
}

CloudFileCachePtr FileCacheFactory::getByPath(const IFileCache::Key& key) {
    return _caches[KeyHash()(key) % _caches.size()].get();
}

std::vector<IFileCache::QueryContextHolderPtr> FileCacheFactory::get_query_context_holders(
        const TUniqueId& query_id) {
    std::vector<IFileCache::QueryContextHolderPtr> holders;
    for (const auto& cache : _caches) {
        holders.push_back(cache->get_query_context_holder(query_id));
    }
    return holders;
}

} // namespace io
} // namespace doris
