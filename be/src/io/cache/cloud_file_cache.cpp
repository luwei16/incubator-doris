#include "io/cache/cloud_file_cache.h"

#include <filesystem>

#include "io/cache/file_cache_fwd.h"
#include "io/cache/file_cache_settings.h"
#include "vec/common/hex.h"
#include "vec/common/sip_hash.h"

namespace fs = std::filesystem;

namespace doris {
namespace io {

IFileCache::IFileCache(const std::string& cache_base_path, const FileCacheSettings& cache_settings)
        : _cache_base_path(cache_base_path),
          _max_size(cache_settings.max_size),
          _max_element_size(cache_settings.max_elements),
          _max_file_segment_size(cache_settings.max_file_segment_size) {}

std::string IFileCache::Key::to_string() const {
    return vectorized::get_hex_uint_lowercase(key);
}

IFileCache::Key IFileCache::hash(const std::string& path) {
    uint128_t key;
    sip_hash128(path.data(), path.size(), reinterpret_cast<char*>(&key));
    return Key(key);
}

std::string IFileCache::get_path_in_local_cache(const Key& key, size_t offset) const {
    auto key_str = key.to_string();
    return fs::path(_cache_base_path) / key_str.substr(0, 3) / key_str / std::to_string(offset);
}

std::string IFileCache::get_path_in_local_cache(const Key& key) const {
    auto key_str = key.to_string();
    return fs::path(_cache_base_path) / key_str.substr(0, 3) / key_str;
}

} // namespace io
} // namespace doris
