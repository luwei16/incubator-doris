// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "io/fs/cached_remote_file_reader.h"

#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>

#include <memory>
#include <utility>

#include "io/cache/cloud_file_cache.h"
#include "io/cache/file_cache_factory.h"
#include "io/cache/file_cache_fwd.h"
#include "io/fs/s3_common.h"
#include "util/doris_metrics.h"
#include "vec/common/sip_hash.h"

namespace doris {
namespace io {

CachedRemoteFileReader::CachedRemoteFileReader(FileReaderSPtr remote_file_reader)
        : _remote_file_reader(std::move(remote_file_reader)) {
    _cache_key = IFileCache::hash(path().filename().native());
    _cache = FileCacheFactory::instance().getByPath(_cache_key);
}

CachedRemoteFileReader::~CachedRemoteFileReader() {
    close();
}

Status CachedRemoteFileReader::close() {
    return _remote_file_reader->close();
}

std::pair<size_t, size_t> CachedRemoteFileReader::_align_size(size_t offset,
                                                              size_t read_size) const {
    size_t left = offset;
    size_t right = offset + read_size - 1;
    size_t align_left = (left / REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE) *
                        REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE;
    size_t align_right = (right / REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE + 1) *
                         REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE;
    align_right = align_right < size() ? align_right : size();
    size_t align_size = align_right - align_left;
    return std::make_pair(align_left, align_size);
}

Status CachedRemoteFileReader::read_at(size_t offset, Slice result, size_t* bytes_read) {
    DCHECK(!closed());
    if (offset > size()) {
        return Status::IOError(
                fmt::format("offset exceeds file size(offset: {), file size: {}, path: {})", offset,
                            size(), path().native()));
    }
    size_t bytes_req = result.size;
    bytes_req = std::min(bytes_req, size() - offset);
    if (UNLIKELY(bytes_req == 0)) {
        *bytes_read = 0;
        return Status::OK();
    }
    auto [align_left, align_size] = _align_size(offset, bytes_req);
    DCHECK((align_left % REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE) == 0);
    FileSegmentsHolder holder = _cache->get_or_set(_cache_key, align_left, align_size);
    std::vector<FileSegmentSPtr> empty_segments;
    for (auto& segment : holder.file_segments) {
        if (segment->state() == FileSegment::State::EMPTY) {
            segment->get_or_set_downloader();
            if (segment->is_downloader()) {
                empty_segments.push_back(segment);
            }
        } else if (segment->state() == FileSegment::State::SKIP_CACHE) {
            empty_segments.push_back(segment);
        }
    }

    size_t empty_start = 0;
    size_t empty_end = 0;
    if (!empty_segments.empty()) {
        empty_start = empty_segments.front()->range().left;
        empty_end = empty_segments.back()->range().right;
        size_t size = empty_end - empty_start + 1;
        std::unique_ptr<char[]> buffer(new char[size]);
        RETURN_IF_ERROR(
                _remote_file_reader->read_at(empty_start, Slice(buffer.get(), size), &size));
        for (auto& segment : empty_segments) {
            if (segment->state() == FileSegment::State::SKIP_CACHE) {
                continue;
            }
            char* cur_ptr = buffer.get() + segment->range().left - empty_start;
            size_t segment_size = segment->range().size();
            segment->reserve(segment_size);
            segment->append(Slice(cur_ptr, segment_size));
            segment->finalize_write();
        }
        // copy from memory directly
        size_t copy_left_offset = offset < empty_start ? empty_start : offset;
        char* dst = result.data + (copy_left_offset - offset);
        char* src = buffer.get() + (copy_left_offset - empty_start);
        size_t right_offset = offset + result.size - 1;
        size_t copy_right_offset = right_offset < empty_end ? right_offset : empty_end;
        size_t copy_size = copy_right_offset - copy_left_offset + 1;
        memcpy(dst, src, copy_size);
    }

    size_t current_offset = offset;
    size_t end_offset = offset + bytes_req - 1;
    *bytes_read = 0;
    for (auto& segment : holder.file_segments) {
        size_t left = segment->range().left;
        size_t right = segment->range().right;
        size_t read_size =
                end_offset > right ? right - current_offset + 1 : end_offset - current_offset + 1;
        if (empty_start <= left && right <= empty_end) {
            *bytes_read += read_size;
            current_offset = right + 1;
            continue;
        }
        while (segment->wait() != FileSegment::State::DOWNLOADED) {
        }
        size_t file_offset = current_offset - left;
        segment->read_at(Slice(result.data + (current_offset - offset), read_size), file_offset);
        *bytes_read += read_size;
        current_offset = right + 1;
    }
    DCHECK(*bytes_read == bytes_req);
    DorisMetrics::instance()->s3_bytes_read_total->increment(*bytes_read);
    return Status::OK();
}

} // namespace io
} // namespace doris
