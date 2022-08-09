#include "io/cache/lru_file_cache.h"

#include <filesystem>
#include <random>
#include <system_error>

#include "io/cache/cloud_file_cache.h"
#include "io/cache/file_cache_settings.h"
#include "util/time.h"
#include "vec/common/hex.h"
#include "vec/common/sip_hash.h"

namespace fs = std::filesystem;

namespace doris {
namespace io {

LRUFileCache::LRUFileCache(const std::string& cache_base_path_,
                           const FileCacheSettings& cache_settings_)
        : IFileCache(cache_base_path_, cache_settings_) {
    initialize();
}

void LRUFileCache::initialize() {
    std::lock_guard cache_lock(_mutex);
    if (!_is_initialized) {
        if (fs::exists(_cache_base_path)) {
            load_cache_info_into_memory(cache_lock);
        } else {
            fs::create_directories(_cache_base_path);
        }
    }
    _is_initialized = true;
}

void LRUFileCache::use_cell(const FileSegmentCell& cell, FileSegments& result,
                            std::lock_guard<std::mutex>& cache_lock) {
    auto file_segment = cell.file_segment;

    DCHECK(!(file_segment->is_downloaded() &&
             fs::file_size(get_path_in_local_cache(file_segment->key(), file_segment->offset())) ==
                     0))
            << "Cannot have zero size downloaded file segments. Current file segment: "
            << file_segment->range().to_string();

    result.push_back(cell.file_segment);

    /**
     * A cell receives a queue iterator on first successful space reservation attempt
     * (space is reserved incrementally on each read buffer nextImpl() call).
     */
    if (cell.queue_iterator) {
        /// Move to the end of the queue. The iterator remains valid.
        _queue.move_to_end(*cell.queue_iterator, cache_lock);
    }
}

LRUFileCache::FileSegmentCell* LRUFileCache::get_cell(
        const Key& key, size_t offset, std::lock_guard<std::mutex>& /* cache_lock */) {
    auto it = _files.find(key);
    if (it == _files.end()) {
        return nullptr;
    }

    auto& offsets = it->second;
    auto cell_it = offsets.find(offset);
    if (cell_it == offsets.end()) {
        return nullptr;
    }

    return &cell_it->second;
}

FileSegments LRUFileCache::get_impl(const Key& key, const FileSegment::Range& range,
                                    std::lock_guard<std::mutex>& cache_lock) {
    /// Given range = [left, right] and non-overlapping ordered set of file segments,
    /// find list [segment1, ..., segmentN] of segments which intersect with given range.

    auto it = _files.find(key);
    if (it == _files.end()) {
        return {};
    }

    const auto& file_segments = it->second;
    if (file_segments.empty()) {
        auto key_path = get_path_in_local_cache(key);

        _files.erase(key);

        /// Note: it is guaranteed that there is no concurrency with files deletion,
        /// because cache files are deleted only inside IFileCache and under cache lock.
        if (fs::exists(key_path)) {
            std::error_code ec;
            fs::remove_all(key_path, ec);
            if (ec) {
                LOG(WARNING) << ec.message();
            }
        }

        return {};
    }

    FileSegments result;
    auto segment_it = file_segments.lower_bound(range.left);
    if (segment_it == file_segments.end()) {
        /// N - last cached segment for given file key, segment{N}.offset < range.left:
        ///   segment{N}                       segment{N}
        /// [________                         [_______]
        ///     [__________]         OR                  [________]
        ///     ^                                        ^
        ///     range.left                               range.left

        const auto& cell = file_segments.rbegin()->second;
        if (cell.file_segment->range().right < range.left) {
            return {};
        }

        use_cell(cell, result, cache_lock);
    } else { /// segment_it <-- segmment{k}
        if (segment_it != file_segments.begin()) {
            const auto& prev_cell = std::prev(segment_it)->second;
            const auto& prev_cell_range = prev_cell.file_segment->range();

            if (range.left <= prev_cell_range.right) {
                ///   segment{k-1}  segment{k}
                ///   [________]   [_____
                ///       [___________
                ///       ^
                ///       range.left

                use_cell(prev_cell, result, cache_lock);
            }
        }

        ///  segment{k} ...       segment{k-1}  segment{k}                      segment{k}
        ///  [______              [______]     [____                        [________
        ///  [_________     OR              [________      OR    [______]   ^
        ///  ^                              ^                           ^   segment{k}.offset
        ///  range.left                     range.left                  range.right

        while (segment_it != file_segments.end()) {
            const auto& cell = segment_it->second;
            if (range.right < cell.file_segment->range().left) {
                break;
            }

            use_cell(cell, result, cache_lock);
            ++segment_it;
        }
    }

    return result;
}

FileSegments LRUFileCache::split_range_into_cells(const Key& key, size_t offset, size_t size,
                                                  FileSegment::State state,
                                                  std::lock_guard<std::mutex>& cache_lock) {
    assert(size > 0);

    auto current_pos = offset;
    auto end_pos_non_included = offset + size;

    size_t current_size = 0;
    size_t remaining_size = size;

    FileSegments file_segments;
    while (current_pos < end_pos_non_included) {
        current_size = std::min(remaining_size, _max_file_segment_size);
        remaining_size -= current_size;
        state = try_reserve(key, offset, size, cache_lock) ? state : FileSegment::State::SKIP_CACHE;
        if (UNLIKELY(state == FileSegment::State::SKIP_CACHE)) {
            auto file_segment = std::make_shared<FileSegment>(current_pos, current_size, key, this,
                                                              FileSegment::State::SKIP_CACHE);
            file_segments.push_back(std::move(file_segment));
        } else {
            auto* cell = add_cell(key, current_pos, current_size, state, cache_lock);
            if (cell) {
                file_segments.push_back(cell->file_segment);
            }
        }

        current_pos += current_size;
    }

    assert(file_segments.empty() || offset + size - 1 == file_segments.back()->range().right);
    return file_segments;
}

void LRUFileCache::fill_holes_with_empty_file_segments(FileSegments& file_segments, const Key& key,
                                                       const FileSegment::Range& range,
                                                       std::lock_guard<std::mutex>& cache_lock) {
    /// There are segments [segment1, ..., segmentN]
    /// (non-overlapping, non-empty, ascending-ordered) which (maybe partially)
    /// intersect with given range.

    /// It can have holes:
    /// [____________________]         -- requested range
    ///     [____]  [_]   [_________]  -- intersecting cache [segment1, ..., segmentN]
    ///
    /// For each such hole create a cell with file segment state EMPTY.

    auto it = file_segments.begin();
    auto segment_range = (*it)->range();

    size_t current_pos;
    if (segment_range.left < range.left) {
        ///    [_______     -- requested range
        /// [_______
        /// ^
        /// segment1

        current_pos = segment_range.right + 1;
        ++it;
    } else {
        current_pos = range.left;
    }

    while (current_pos <= range.right && it != file_segments.end()) {
        segment_range = (*it)->range();

        if (current_pos == segment_range.left) {
            current_pos = segment_range.right + 1;
            ++it;
            continue;
        }

        assert(current_pos < segment_range.left);

        auto hole_size = segment_range.left - current_pos;

        file_segments.splice(it, split_range_into_cells(key, current_pos, hole_size,
                                                        FileSegment::State::EMPTY, cache_lock));

        current_pos = segment_range.right + 1;
        ++it;
    }

    if (current_pos <= range.right) {
        ///   ________]     -- requested range
        ///   _____]
        ///        ^
        /// segmentN

        auto hole_size = range.right - current_pos + 1;

        file_segments.splice(file_segments.end(),
                             split_range_into_cells(key, current_pos, hole_size,
                                                    FileSegment::State::EMPTY, cache_lock));
    }
}

FileSegmentsHolder LRUFileCache::get_or_set(const Key& key, size_t offset, size_t size) {
    FileSegment::Range range(offset, offset + size - 1);

    std::lock_guard cache_lock(_mutex);

    /// Get all segments which intersect with the given range.
    auto file_segments = get_impl(key, range, cache_lock);

    if (file_segments.empty()) {
        file_segments =
                split_range_into_cells(key, offset, size, FileSegment::State::EMPTY, cache_lock);
    } else {
        fill_holes_with_empty_file_segments(file_segments, key, range, cache_lock);
    }

    assert(!file_segments.empty());
    return FileSegmentsHolder(std::move(file_segments));
}

LRUFileCache::FileSegmentCell* LRUFileCache::add_cell(const Key& key, size_t offset, size_t size,
                                                      FileSegment::State state,
                                                      std::lock_guard<std::mutex>& cache_lock) {
    /// Create a file segment cell and put it in `files` map by [key][offset].

    if (!size) {
        return nullptr; /// Empty files are not cached.
    }

    DCHECK(_files[key].count(offset) == 0)
            << "Cache already exists for key: " << key.to_string() << ", offset: " << offset
            << ", size: " << size
            << ".\nCurrent cache structure: " << dump_structure_unlocked(key, cache_lock);

    FileSegmentCell cell(std::make_shared<FileSegment>(offset, size, key, this, state), this,
                         cache_lock);
    auto& offsets = _files[key];

    if (offsets.empty()) {
        auto key_path = get_path_in_local_cache(key);
        if (!fs::exists(key_path)) {
            fs::create_directories(key_path);
        }
    }

    auto [it, inserted] = offsets.insert({offset, std::move(cell)});

    cell.queue_iterator = _queue.add(key, offset, size, cache_lock);

    DCHECK(inserted) << "Failed to insert into cache key: " << key.to_string()
                     << ", offset: " << offset << ", size: " << size;

    return &(it->second);
}

bool LRUFileCache::try_reserve(const Key& key, size_t offset, size_t size,
                               std::lock_guard<std::mutex>& cache_lock) {
    return try_reserve_for_main_list(key, offset, size, cache_lock);
}

bool LRUFileCache::try_reserve_for_main_list(const Key& key, size_t offset, size_t size,
                                             std::lock_guard<std::mutex>& cache_lock) {
    auto removed_size = 0;
    size_t queue_size = _queue.get_elements_num(cache_lock);
    assert(queue_size <= _max_element_size);

    auto is_overflow = [&] {
        /// max_size == 0 means unlimited cache size, max_element_size means unlimited number of cache elements.
        return (_max_size != 0 &&
                _queue.get_total_cache_size(cache_lock) + size - removed_size > _max_size) ||
               (_max_element_size != 0 && queue_size >= _max_element_size);
    };

    std::vector<FileSegmentCell*> to_evict;
    std::vector<FileSegmentCell*> trash;

    for (const auto& [entry_key, entry_offset, entry_size] : _queue) {
        if (!is_overflow()) {
            break;
        }

        auto* cell = get_cell(entry_key, entry_offset, cache_lock);

        DCHECK(cell) << "Cache became inconsistent. Key: " << key.to_string()
                     << ", offset: " << offset;

        size_t cell_size = cell->size();
        assert(entry_size == cell_size);

        /// It is guaranteed that cell is not removed from cache as long as
        /// pointer to corresponding file segment is hold by any other thread.

        if (cell->releasable()) {
            auto& file_segment = cell->file_segment;

            std::lock_guard segment_lock(file_segment->_mutex);

            switch (file_segment->_download_state) {
            case FileSegment::State::DOWNLOADED: {
                /// Cell will actually be removed only if
                /// we managed to reserve enough space.

                to_evict.push_back(cell);
                break;
            }
            default: {
                trash.push_back(cell);
                break;
            }
            }

            removed_size += cell_size;
            --queue_size;
        }
    }

    auto remove_file_segment = [&](FileSegmentSPtr file_segment) {
        std::lock_guard segment_lock(file_segment->_mutex);
        remove(file_segment->key(), file_segment->offset(), cache_lock, segment_lock);
    };

    /// This case is very unlikely, can happen in case of exception from
    /// file_segment->complete(), which would be a logical error.
    for (auto& cell : trash) {
        if (auto file_segment = cell->file_segment) {
            remove_file_segment(file_segment);
        }
    }

    for (auto& cell : to_evict) {
        if (auto file_segment = cell->file_segment) {
            remove_file_segment(file_segment);
        }
    }

    if (is_overflow()) {
        return false;
    }

    DCHECK(_queue.get_total_cache_size(cache_lock) <= (1ull << 63))
            << "Cache became inconsistent. There must be a bug";

    return true;
}

void LRUFileCache::remove_if_exists(const Key& key) {
    std::lock_guard cache_lock(_mutex);

    auto it = _files.find(key);
    if (it == _files.end()) {
        return;
    }

    auto& offsets = it->second;

    std::vector<FileSegmentCell*> to_remove;
    to_remove.reserve(offsets.size());

    for (auto& [offset, cell] : offsets) {
        to_remove.push_back(&cell);
    }

    bool some_cells_were_skipped = false;
    for (auto& cell : to_remove) {
        /// In ordinary case we remove data from cache when it's not used by anyone.
        /// But if we have multiple replicated zero-copy tables on the same server
        /// it became possible to start removing something from cache when it is used
        /// by other "zero-copy" tables. That is why it's not an error.
        if (!cell->releasable()) {
            some_cells_were_skipped = true;
            continue;
        }

        auto file_segment = cell->file_segment;
        if (file_segment) {
            std::lock_guard<std::mutex> segment_lock(file_segment->_mutex);
            remove(file_segment->key(), file_segment->offset(), cache_lock, segment_lock);
        }
    }

    auto key_path = get_path_in_local_cache(key);

    if (!some_cells_were_skipped) {
        _files.erase(key);

        if (fs::exists(key_path)) {
            std::error_code ec;
            fs::remove_all(key_path, ec);
            if (ec) {
                LOG(WARNING) << ec.message();
            }
        }
    }
}

void LRUFileCache::remove_if_releasable() {
    /// Try remove all cached files by cache_base_path.
    /// Only releasable file segments are evicted.
    /// `remove_persistent_files` defines whether non-evictable by some criteria files
    /// (they do not comply with the cache eviction policy) should also be removed.

    std::lock_guard cache_lock(_mutex);

    std::vector<FileSegment*> to_remove;
    for (auto it = _queue.begin(); it != _queue.end();) {
        const auto& [key, offset, size] = *it++;
        auto* cell = get_cell(key, offset, cache_lock);

        DCHECK(cell) << "Cache is in inconsistent state: LRU queue contains entries with no "
                        "cache cell";

        if (cell->releasable()) {
            auto file_segment = cell->file_segment;
            if (file_segment) {
                std::lock_guard segment_lock(file_segment->_mutex);
                remove(file_segment->key(), file_segment->offset(), cache_lock, segment_lock);
            }
        }
    }
}

void LRUFileCache::remove(Key key, size_t offset, std::lock_guard<std::mutex>& cache_lock,
                          std::lock_guard<std::mutex>& /* segment_lock */) {
    auto* cell = get_cell(key, offset, cache_lock);
    DCHECK(cell) << "No cache cell for key: " << key.to_string() << ", offset: " << offset;

    if (cell->queue_iterator) {
        _queue.remove(*cell->queue_iterator, cache_lock);
    }

    auto& offsets = _files[key];
    offsets.erase(offset);

    auto cache_file_path = get_path_in_local_cache(key, offset);
    if (fs::exists(cache_file_path)) {
        std::error_code ec;
        fs::remove(cache_file_path, ec);
        if (ec) {
            LOG(WARNING) << ec.message();
        }

        if (_is_initialized && offsets.empty()) {
            auto key_path = get_path_in_local_cache(key);

            _files.erase(key);

            if (fs::exists(key_path)) {
                std::error_code ec;
                fs::remove_all(key_path, ec);
                if (ec) {
                    LOG(WARNING) << ec.message();
                }
            }
        }
    }
}

void LRUFileCache::load_cache_info_into_memory(std::lock_guard<std::mutex>& cache_lock) {
    Key key;
    uint64_t offset = 0;
    size_t size = 0;
    std::vector<std::pair<LRUQueue::Iterator, std::weak_ptr<FileSegment>>> queue_entries;

    /// cache_base_path / key_prefix / key / offset
    fs::directory_iterator key_prefix_it {_cache_base_path};
    for (; key_prefix_it != fs::directory_iterator(); ++key_prefix_it) {
        fs::directory_iterator key_it {key_prefix_it->path()};
        for (; key_it != fs::directory_iterator(); ++key_it) {
            key = Key(
                    vectorized::unhex_uint<uint128_t>(key_it->path().filename().native().c_str()));

            fs::directory_iterator offset_it {key_it->path()};
            for (; offset_it != fs::directory_iterator(); ++offset_it) {
                auto offset_with_suffix = offset_it->path().filename().native();
                bool parsed = true;
                try {
                    offset = stoull(offset_with_suffix);
                } catch (...) {
                    parsed = false;
                }

                if (!parsed) {
                    LOG(WARNING) << "Unexpected file: " << offset_it->path().native();
                    continue; /// Or just remove? Some unexpected file.
                }

                size = offset_it->file_size();
                if (!size) {
                    std::error_code ec;
                    fs::remove(offset_it->path(), ec);
                    if (ec) {
                        LOG(WARNING) << ec.message();
                    }
                    continue;
                }

                if (try_reserve(key, offset, size, cache_lock)) {
                    auto* cell =
                            add_cell(key, offset, size, FileSegment::State::DOWNLOADED, cache_lock);
                    if (cell) {
                        queue_entries.emplace_back(*cell->queue_iterator, cell->file_segment);
                    }
                } else {
                    LOG(WARNING) << "Cache capacity changed (max size: " << _max_size
                                 << ", available: " << get_available_cache_size_unlocked(cache_lock)
                                 << "), cached file " << key_it->path().string()
                                 << " does not fit in cache anymore (size: " << size << ")";
                    std::error_code ec;
                    fs::remove(offset_it->path(), ec);
                    if (ec) {
                        LOG(WARNING) << ec.message();
                    }
                }
            }
        }
    }

    /// Shuffle cells to have random order in LRUQueue as at startup all cells have the same priority.
    auto rng = std::default_random_engine {static_cast<uint64_t>(MonotonicNanos())};
    std::shuffle(queue_entries.begin(), queue_entries.end(), rng);
    for (const auto& [it, file_segment] : queue_entries) {
        /// Cell cache size changed and, for example, 1st file segment fits into cache
        /// and 2nd file segment will fit only if first was evicted, then first will be removed and
        /// cell is nullptr here.
        if (file_segment.expired()) {
            continue;
        }

        _queue.move_to_end(it, cache_lock);
    }
}

std::vector<std::string> LRUFileCache::try_get_cache_paths(const Key& key) {
    std::lock_guard cache_lock(_mutex);

    std::vector<std::string> cache_paths;

    const auto& cells_by_offset = _files[key];

    for (const auto& [offset, cell] : cells_by_offset) {
        if (cell.file_segment->state() == FileSegment::State::DOWNLOADED) {
            cache_paths.push_back(get_path_in_local_cache(key, offset));
        }
    }

    return cache_paths;
}

size_t LRUFileCache::get_used_cache_size() const {
    std::lock_guard cache_lock(_mutex);
    return get_used_cache_size_unlocked(cache_lock);
}

size_t LRUFileCache::get_used_cache_size_unlocked(std::lock_guard<std::mutex>& cache_lock) const {
    return _queue.get_total_cache_size(cache_lock);
}

size_t LRUFileCache::get_available_cache_size() const {
    std::lock_guard cache_lock(_mutex);
    return get_available_cache_size_unlocked(cache_lock);
}

size_t LRUFileCache::get_available_cache_size_unlocked(
        std::lock_guard<std::mutex>& cache_lock) const {
    return _max_size - get_used_cache_size_unlocked(cache_lock);
}

size_t LRUFileCache::get_file_segments_num() const {
    std::lock_guard cache_lock(_mutex);
    return get_file_segments_num_unlocked(cache_lock);
}

size_t LRUFileCache::get_file_segments_num_unlocked(std::lock_guard<std::mutex>& cache_lock) const {
    return _queue.get_elements_num(cache_lock);
}

LRUFileCache::FileSegmentCell::FileSegmentCell(FileSegmentSPtr file_segment_, LRUFileCache* cache,
                                               std::lock_guard<std::mutex>& cache_lock)
        : file_segment(file_segment_) {
    /**
     * Cell can be created with either DOWNLOADED or EMPTY file segment's state.
     * File segment acquires DOWNLOADING state and creates LRUQueue iterator on first
     * successful getOrSetDownaloder call.
     */

    switch (file_segment->_download_state) {
    case FileSegment::State::DOWNLOADED: {
        break;
    }
    case FileSegment::State::EMPTY: {
        break;
    }
    default:
        DCHECK(false) << "Can create cell with either EMPTY, DOWNLOADED, DOWNLOADING state, got: "
                      << FileSegment::state_to_string(file_segment->_download_state);
    }
}

IFileCache::LRUQueue::Iterator IFileCache::LRUQueue::add(
        const IFileCache::Key& key, size_t offset, size_t size,
        std::lock_guard<std::mutex>& /* cache_lock */) {
    cache_size += size;
    return queue.insert(queue.end(), FileKeyAndOffset(key, offset, size));
}

void IFileCache::LRUQueue::remove(Iterator queue_it,
                                  std::lock_guard<std::mutex>& /* cache_lock */) {
    cache_size -= queue_it->size;
    queue.erase(queue_it);
}

void IFileCache::LRUQueue::remove_all(std::lock_guard<std::mutex>& /* cache_lock */) {
    queue.clear();
    cache_size = 0;
}

void IFileCache::LRUQueue::move_to_end(Iterator queue_it,
                                       std::lock_guard<std::mutex>& /* cache_lock */) {
    queue.splice(queue.end(), queue, queue_it);
}

void IFileCache::LRUQueue::increment_size(Iterator queue_it, size_t size_increment,
                                          std::lock_guard<std::mutex>& /* cache_lock */) {
    cache_size += size_increment;
    queue_it->size += size_increment;
}

bool IFileCache::LRUQueue::contains(const IFileCache::Key& key, size_t offset,
                                    std::lock_guard<std::mutex>& /* cache_lock */) const {
    /// This method is used for assertions in debug mode.
    /// So we do not care about complexity here.
    for (const auto& [entry_key, entry_offset, size] : queue) {
        if (key == entry_key && offset == entry_offset) {
            return true;
        }
    }
    return false;
}

std::string IFileCache::LRUQueue::to_string(std::lock_guard<std::mutex>& /* cache_lock */) const {
    std::string result;
    for (const auto& [key, offset, size] : queue) {
        if (!result.empty()) {
            result += ", ";
        }
        result += fmt::format("{}: [{}, {}]", key.to_string(), offset, offset + size - 1);
    }
    return result;
}

std::string LRUFileCache::dump_structure(const Key& key) {
    std::lock_guard cache_lock(_mutex);
    return dump_structure_unlocked(key, cache_lock);
}

std::string LRUFileCache::dump_structure_unlocked(const Key& key,
                                                  std::lock_guard<std::mutex>& cache_lock) {
    std::stringstream result;
    const auto& cells_by_offset = _files[key];

    for (const auto& [offset, cell] : cells_by_offset) {
        result << cell.file_segment->get_info_for_log() << "\n";
    }

    result << "\n\nQueue: " << _queue.to_string(cache_lock);
    return result.str();
}

} // namespace io
} // namespace doris
