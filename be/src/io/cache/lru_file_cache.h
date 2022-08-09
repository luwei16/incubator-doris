#pragma once

#include <chrono>
#include <map>
#include <memory>
#include <optional>
#include <unordered_map>

#include "io/cache/cloud_file_cache.h"
#include "io/cache/file_segment.h"

namespace doris {
namespace io {

/**
 * Local cache for remote filesystem files, represented as a set of non-overlapping non-empty file segments.
 * Implements LRU eviction policy.
 */
class LRUFileCache final : public IFileCache {
public:
    LRUFileCache(const std::string& cache_base_path, const FileCacheSettings& cache_settings);

    FileSegmentsHolder get_or_set(const Key& key, size_t offset, size_t size) override;

    void initialize() override;

    void remove_if_exists(const Key& key) override;

    void remove_if_releasable() override;

    std::vector<std::string> try_get_cache_paths(const Key& key) override;

    size_t get_used_cache_size() const override;

    size_t get_file_segments_num() const override;

private:
    struct FileSegmentCell {
        FileSegmentSPtr file_segment;

        /// Iterator is put here on first reservation attempt, if successful.
        std::optional<LRUQueue::Iterator> queue_iterator;

        /// Pointer to file segment is always hold by the cache itself.
        /// Apart from pointer in cache, it can be hold by cache users, when they call
        /// getorSet(), but cache users always hold it via FileSegmentsHolder.
        bool releasable() const { return file_segment.unique(); }

        size_t size() const { return file_segment->_reserved_size; }

        FileSegmentCell(FileSegmentSPtr file_segment_, LRUFileCache* cache,
                        std::lock_guard<std::mutex>& cache_lock);

        FileSegmentCell(FileSegmentCell&& other) noexcept
                : file_segment(std::move(other.file_segment)),
                  queue_iterator(other.queue_iterator) {}

        FileSegmentCell& operator=(const FileSegmentCell&) = delete;
        FileSegmentCell(const FileSegmentCell&) = delete;
    };

    using FileSegmentsByOffset = std::map<size_t, FileSegmentCell>;
    using CachedFiles = std::unordered_map<Key, FileSegmentsByOffset>;

    CachedFiles _files;
    LRUQueue _queue;

    FileSegments get_impl(const Key& key, const FileSegment::Range& range,
                          std::lock_guard<std::mutex>& cache_lock);

    FileSegmentCell* get_cell(const Key& key, size_t offset,
                              std::lock_guard<std::mutex>& cache_lock);

    FileSegmentCell* add_cell(const Key& key, size_t offset, size_t size, FileSegment::State state,
                              std::lock_guard<std::mutex>& cache_lock);

    void use_cell(const FileSegmentCell& cell, FileSegments& result,
                  std::lock_guard<std::mutex>& cache_lock);

    bool try_reserve(const Key& key, size_t offset, size_t size,
                     std::lock_guard<std::mutex>& cache_lock) override;

    bool try_reserve_for_main_list(const Key& key, size_t offset, size_t size,
                                   std::lock_guard<std::mutex>& cache_lock);

    void remove(Key key, size_t offset, std::lock_guard<std::mutex>& cache_lock,
                std::lock_guard<std::mutex>& segment_lock) override;

    size_t get_available_cache_size() const;

    void load_cache_info_into_memory(std::lock_guard<std::mutex>& cache_lock);

    FileSegments split_range_into_cells(const Key& key, size_t offset, size_t size,
                                        FileSegment::State state,
                                        std::lock_guard<std::mutex>& cache_lock);

    std::string dump_structure_unlocked(const Key& key_, std::lock_guard<std::mutex>& cache_lock);

    void fill_holes_with_empty_file_segments(FileSegments& file_segments, const Key& key,
                                             const FileSegment::Range& range,
                                             std::lock_guard<std::mutex>& cache_lock);

    size_t get_used_cache_size_unlocked(std::lock_guard<std::mutex>& cache_lock) const;

    size_t get_available_cache_size_unlocked(std::lock_guard<std::mutex>& cache_lock) const;

    size_t get_file_segments_num_unlocked(std::lock_guard<std::mutex>& cache_lock) const;

public:
    std::string dump_structure(const Key& key) override;
};

} // namespace io
} // namespace doris
