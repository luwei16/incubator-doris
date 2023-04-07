#include "cloud/io/cloud_file_segment.h"

#include <filesystem>
#include <sstream>
#include <string>
#include <system_error>
#include <thread>

#include "cloud/io/cloud_file_cache.h"
#include "cloud/io/file_writer.h"
#include "cloud/io/local_file_system.h"
#include "common/status.h"
namespace doris {
namespace io {

FileSegment::FileSegment(size_t offset, size_t size, const Key& key, CloudFileCache* cache,
                         State download_state, CacheType cache_type, int64_t expiration_time)
        : _segment_range(offset, offset + size - 1),
          _download_state(download_state),
          _file_key(key),
          _cache(cache),
          _cache_type(cache_type),
          _expiration_time(expiration_time) {
    /// On creation, file segment state can be EMPTY, DOWNLOADED, DOWNLOADING.
    switch (_download_state) {
    /// EMPTY is used when file segment is not in cache and
    /// someone will _potentially_ want to download it (after calling getOrSetDownloader()).
    case State::EMPTY:
    case State::SKIP_CACHE: {
        break;
    }
    /// DOWNLOADED is used either on initial cache metadata load into memory on server startup
    /// or on reduceSizeToDownloaded() -- when file segment object is updated.
    case State::DOWNLOADED: {
        _downloaded_size = size;
        break;
    }
    /// DOWNLOADING is used only for write-through caching (e.g. getOrSetDownloader() is not
    /// needed, downloader is set on file segment creation).
    case State::DOWNLOADING: {
        _downloader_id = get_caller_id();
        break;
    }
    default: {
        DCHECK(false) << "Can create cell with either EMPTY, DOWNLOADED, DOWNLOADING, SKIP_CACHE ";
    }
    }
}

FileSegment::State FileSegment::state() const {
    std::lock_guard segment_lock(_mutex);
    return _download_state;
}

size_t FileSegment::get_download_offset() const {
    std::lock_guard segment_lock(_mutex);
    return range().left + get_downloaded_size(segment_lock);
}

size_t FileSegment::get_downloaded_size() const {
    std::lock_guard segment_lock(_mutex);
    return get_downloaded_size(segment_lock);
}

size_t FileSegment::get_downloaded_size(std::lock_guard<std::mutex>& /* segment_lock */) const {
    if (_download_state == State::DOWNLOADED) {
        return _downloaded_size;
    }

    std::lock_guard download_lock(_download_mutex);
    return _downloaded_size;
}

std::string FileSegment::get_caller_id() {
    std::stringstream ss;
    ss << std::this_thread::get_id();
    return ss.str();
}

std::string FileSegment::get_or_set_downloader() {
    std::lock_guard segment_lock(_mutex);

    if (_downloader_id.empty()) {
        DCHECK(_download_state != State::DOWNLOADING);

        _downloader_id = get_caller_id();
        _download_state = State::DOWNLOADING;
    } else if (_downloader_id == get_caller_id()) {
        LOG(INFO) << "Attempt to set the same downloader for segment " << range().to_string()
                  << " for the second time";
    }

    return _downloader_id;
}

void FileSegment::reset_downloader(std::lock_guard<std::mutex>& segment_lock) {
    DCHECK(!_downloader_id.empty()) << "There is no downloader";

    DCHECK(get_caller_id() == _downloader_id) << "Downloader can be reset only by downloader";

    reset_downloader_impl(segment_lock);
}

void FileSegment::reset_downloader_impl(std::lock_guard<std::mutex>& segment_lock) {
    if (_downloaded_size == range().size()) {
        set_downloaded(segment_lock);
    } else {
        _downloaded_size = 0;
        _download_state = State::EMPTY;
        _downloader_id.clear();
        _cache_writer.reset();
    }
}

std::string FileSegment::get_downloader() const {
    std::lock_guard segment_lock(_mutex);
    return _downloader_id;
}

bool FileSegment::is_downloader() const {
    std::lock_guard segment_lock(_mutex);
    return get_caller_id() == _downloader_id;
}

bool FileSegment::is_downloader_impl(std::lock_guard<std::mutex>& /* segment_lock */) const {
    return get_caller_id() == _downloader_id;
}

Status FileSegment::append(Slice data) {
    DCHECK(data.size != 0) << "Writing zero size is not allowed";
    Status st = Status::OK();
    if (!_cache_writer) {
        auto download_path = get_path_in_local_cache(true);
        st = global_local_filesystem()->create_file(download_path, &_cache_writer);
        if (!st) {
            _cache_writer.reset();
            return st;
        }
    }

    RETURN_IF_ERROR(_cache_writer->append(data));

    std::lock_guard download_lock(_download_mutex);

    _downloaded_size += data.size;
    return st;
}

std::string FileSegment::get_path_in_local_cache(bool is_tmp) const {
    return _cache->get_path_in_local_cache(key(), _expiration_time, offset(), _cache_type, is_tmp);
}

Status FileSegment::read_at(Slice buffer, size_t offset) {
    std::shared_ptr<FileReader> reader;
    if (!(reader = _cache_reader.lock())) {
        std::lock_guard<std::mutex> lock(_mutex);
        if (!(reader = _cache_reader.lock())) {
            auto download_path = get_path_in_local_cache();
            RETURN_IF_ERROR(global_local_filesystem()->open_file(download_path, &reader));
            _cache_reader = CloudFileCache::cache_file_reader(reader);
        }
    }
    size_t bytes_reads = buffer.size;
    RETURN_IF_ERROR(reader->read_at(offset, buffer, &bytes_reads));
    DCHECK(bytes_reads == buffer.size);
    return Status::OK();
}

bool FileSegment::change_cache_type(CacheType new_type) {
    std::unique_lock segment_lock(_mutex);
    if (new_type == _cache_type) {
        return true;
    }
    if (_download_state == State::DOWNLOADED) {
        segment_lock.unlock();
        int retry_time = 10;
        while (retry_time-- && wait() == State::DOWNLOADING) {
        }
        if (retry_time == 10) {
            LOG(WARNING) << fmt::format("Segment change type too long, {} to {}", _cache_type,
                                        new_type);
            return false;
        }
        segment_lock.lock();
    }
    if (_download_state == State::DOWNLOADED) {
        std::error_code ec;
        std::filesystem::rename(
                get_path_in_local_cache(),
                _cache->get_path_in_local_cache(key(), _expiration_time, offset(), new_type), ec);
        if (ec) {
            LOG(ERROR) << ec.message();
            return false;
        }
    }
    _cache_type = new_type;
    return true;
}

Status FileSegment::change_cache_type_self(CacheType new_type) {
    std::unique_lock segment_lock(_mutex);
    Status st = Status::OK();
    if (_cache_type == CacheType::TTL || new_type == _cache_type) {
        return st;
    }
    if (_cache_writer) {
        std::error_code ec;
        std::filesystem::rename(
                get_path_in_local_cache(),
                _cache->get_path_in_local_cache(key(), _expiration_time, offset(), new_type), ec);
        if (ec) {
            LOG(ERROR) << ec.message();
            st = Status::IOError(ec.message());
            return st;
        }
    }
    _cache_type = new_type;
    _cache->change_cache_type(_file_key, _segment_range.left, new_type);
    return st;
}

Status FileSegment::finalize_write() {
    std::lock_guard segment_lock(_mutex);

    RETURN_IF_ERROR(set_downloaded(segment_lock));
    _cv.notify_all();
    return Status::OK();
}

FileSegment::State FileSegment::wait() {
    std::unique_lock segment_lock(_mutex);

    if (_downloader_id.empty()) {
        return _download_state;
    }

    if (_download_state == State::DOWNLOADING) {
        DCHECK(!_downloader_id.empty());
        DCHECK(_downloader_id != get_caller_id());

        _cv.wait_for(segment_lock, std::chrono::seconds(1));
    }

    return _download_state;
}

Status FileSegment::set_downloaded(std::lock_guard<std::mutex>& /* segment_lock */) {
    Status status = Status::OK();
    if (_is_downloaded) {
        return status;
    }

    if (_cache_writer) {
        RETURN_IF_ERROR(_cache_writer->close(false));
        std::error_code ec;
        std::filesystem::rename(_cache_writer->path(), get_path_in_local_cache(), ec);
        if (ec) {
            LOG(ERROR) << fmt::format("failed to rename {} to {} : {}",
                                      _cache_writer->path().string(), get_path_in_local_cache(),
                                      ec.message());
            status = Status::IOError(ec.message());
        }
        _cache_writer.reset();
    }

    if (status) [[likely]] {
        _is_downloaded = true;
        _download_state = State::DOWNLOADED;
    } else {
        _download_state = State::EMPTY;
    }
    _downloader_id.clear();
    return status;
}

void FileSegment::reset_range() {
    size_t old_size = _segment_range.size();
    _segment_range.right = _downloaded_size == _segment_range.size()
                                   ? _segment_range.right
                                   : _segment_range.left + _downloaded_size - 1;
    // new size must be smaller than old size or equal
    size_t new_size = _segment_range.size();
    DCHECK(new_size <= old_size);
    if (new_size < old_size) {
        _cache->reset_range(_file_key, _segment_range.left, old_size, new_size);
    }
}

void FileSegment::complete_unlocked(std::lock_guard<std::mutex>& segment_lock) {
    if (is_downloader_impl(segment_lock)) {
        reset_downloader(segment_lock);
        _cv.notify_all();
    }
}

std::string FileSegment::get_info_for_log() const {
    std::lock_guard segment_lock(_mutex);
    return get_info_for_log_impl(segment_lock);
}

std::string FileSegment::get_info_for_log_impl(std::lock_guard<std::mutex>& segment_lock) const {
    std::stringstream info;
    info << "File segment: " << range().to_string() << ", ";
    info << "state: " << state_to_string(_download_state) << ", ";
    info << "downloaded size: " << get_downloaded_size(segment_lock) << ", ";
    return info.str();
}

std::string FileSegment::state_to_string(FileSegment::State state) {
    switch (state) {
    case FileSegment::State::DOWNLOADED:
        return "DOWNLOADED";
    case FileSegment::State::EMPTY:
        return "EMPTY";
    case FileSegment::State::DOWNLOADING:
        return "DOWNLOADING";
    case FileSegment::State::SKIP_CACHE:
        return "SKIP_CACHE";
    default:
        DCHECK(false);
        return "";
    }
}

bool FileSegment::has_finalized_state() const {
    return _download_state == State::DOWNLOADED;
}

FileSegment::State FileSegment::state_unlock(std::lock_guard<std::mutex>&) const {
    return _download_state;
}

FileSegmentsHolder::~FileSegmentsHolder() {
    /// In CacheableReadBufferFromRemoteFS file segment's downloader removes file segments from
    /// FileSegmentsHolder right after calling file_segment->complete(), so on destruction here
    /// remain only uncompleted file segments.

    CloudFileCache* cache = nullptr;

    for (auto file_segment_it = file_segments.begin(); file_segment_it != file_segments.end();) {
        auto current_file_segment_it = file_segment_it;
        auto& file_segment = *current_file_segment_it;

        if (!cache) {
            cache = file_segment->_cache;
        }

        {
            std::lock_guard cache_lock(cache->_mutex);
            std::lock_guard segment_lock(file_segment->_mutex);
            file_segment->complete_unlocked(segment_lock);
            if (file_segment->state_unlock(segment_lock) == FileSegment::State::EMPTY) {
                // one in cache, one in here
                if (file_segment.use_count() == 2) {
                    cache->remove(file_segment, cache_lock, segment_lock);
                }
            }
        }

        file_segment_it = file_segments.erase(current_file_segment_it);
    }
}

std::string FileSegmentsHolder::to_string() {
    std::string ranges;
    for (const auto& file_segment : file_segments) {
        if (!ranges.empty()) {
            ranges += ", ";
        }
        ranges += file_segment->range().to_string();
    }
    return ranges;
}

} // namespace io
} // namespace doris
