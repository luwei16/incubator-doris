#pragma once

#include <list>
#include <unordered_map>

#include "io/cache/file_cache_fwd.h"

namespace doris {
namespace io {
class FileSegment;
using FileSegmentSPtr = std::shared_ptr<FileSegment>;
using FileSegments = std::list<FileSegmentSPtr>;
struct FileSegmentsHolder;
struct ReadSettings;

/**
 * Local cache for remote filesystem files, represented as a set of non-overlapping non-empty file segments.
 */
class IFileCache {
    friend class FileSegment;
    friend struct FileSegmentsHolder;

public:
    struct Key {
        uint128_t key;
        std::string to_string() const;

        Key() = default;
        explicit Key(const uint128_t& key_) : key(key_) {}

        bool operator==(const Key& other) const { return key == other.key; }
    };

    IFileCache(const std::string& cache_base_path, const FileCacheSettings& cache_settings);

    virtual ~IFileCache() = default;

    /// Restore cache from local filesystem.
    virtual void initialize() = 0;

    virtual void remove_if_exists(const Key& key) = 0;

    virtual void remove_if_releasable() = 0;

    /// Cache capacity in bytes.
    size_t capacity() const { return _max_size; }

    static Key hash(const std::string& path);

    std::string get_path_in_local_cache(const Key& key, size_t offset) const;

    std::string get_path_in_local_cache(const Key& key) const;

    const std::string& get_base_path() const { return _cache_base_path; }

    virtual std::vector<std::string> try_get_cache_paths(const Key& key) = 0;

    /**
     * Given an `offset` and `size` representing [offset, offset + size) bytes interval,
     * return list of cached non-overlapping non-empty
     * file segments `[segment1, ..., segmentN]` which intersect with given interval.
     *
     * Segments in returned list are ordered in ascending order and represent a full contiguous
     * interval (no holes). Each segment in returned list has state: DOWNLOADED, DOWNLOADING or EMPTY.
     *
     * As long as pointers to returned file segments are hold
     * it is guaranteed that these file segments are not removed from cache.
     */
    virtual FileSegmentsHolder get_or_set(const Key& key, size_t offset, size_t size) = 0;

    /// For debug.
    virtual std::string dump_structure(const Key& key) = 0;

    virtual size_t get_used_cache_size() const = 0;

    virtual size_t get_file_segments_num() const = 0;

    IFileCache& operator=(const IFileCache&) = delete;
    IFileCache(const IFileCache&) = delete;

protected:
    std::string _cache_base_path;
    size_t _max_size;
    size_t _max_element_size;
    size_t _max_file_segment_size;

    bool _is_initialized = false;

    mutable std::mutex _mutex;

    virtual bool try_reserve(const Key& key, size_t offset, size_t size,
                             std::lock_guard<std::mutex>& cache_lock) = 0;

    virtual void remove(Key key, size_t offset, std::lock_guard<std::mutex>& cache_lock,
                        std::lock_guard<std::mutex>& segment_lock) = 0;

    class LRUQueue {
    public:
        struct FileKeyAndOffset {
            Key key;
            size_t offset;
            size_t size;

            FileKeyAndOffset(const Key& key_, size_t offset_, size_t size_)
                    : key(key_), offset(offset_), size(size_) {}
        };

        using Iterator = typename std::list<FileKeyAndOffset>::iterator;

        size_t get_total_cache_size(std::lock_guard<std::mutex>& /* cache_lock */) const {
            return cache_size;
        }

        size_t get_elements_num(std::lock_guard<std::mutex>& /* cache_lock */) const {
            return queue.size();
        }

        Iterator add(const Key& key, size_t offset, size_t size,
                     std::lock_guard<std::mutex>& cache_lock);

        void remove(Iterator queue_it, std::lock_guard<std::mutex>& cache_lock);

        void move_to_end(Iterator queue_it, std::lock_guard<std::mutex>& cache_lock);

        /// Space reservation for a file segment is incremental, so we need to be able to increment size of the queue entry.
        void increment_size(Iterator queue_it, size_t size_increment,
                            std::lock_guard<std::mutex>& cache_lock);

        std::string to_string(std::lock_guard<std::mutex>& cache_lock) const;

        bool contains(const Key& key, size_t offset, std::lock_guard<std::mutex>& cache_lock) const;

        Iterator begin() { return queue.begin(); }

        Iterator end() { return queue.end(); }

        void remove_all(std::lock_guard<std::mutex>& cache_lock);

    private:
        std::list<FileKeyAndOffset> queue;
        size_t cache_size = 0;
    };
};

using FileCacheSPtr = std::shared_ptr<IFileCache>;

} // namespace io
} // namespace doris

namespace std {
template <>
struct hash<doris::io::IFileCache::Key> {
    std::size_t operator()(const doris::io::IFileCache::Key& k) const {
        return hash<doris::io::uint128_t>()(k.key);
    }
};
} // namespace std
