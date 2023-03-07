#pragma once

#include <gen_cpp/internal_service.pb.h>
#include <glog/logging.h>

#include <atomic>
#include <condition_variable>

#include "common/status.h"
#include "cloud/io/s3_file_system.h"

namespace Aws::Transfer {
class TransferManager;
} // namespace Aws::Transfer

namespace doris::io {

class FileCacheSegmentDownloader {
public:
    FileCacheSegmentDownloader() {
        _download_thread = std::thread(&FileCacheSegmentDownloader::polling_download_task, this);
    }

    ~FileCacheSegmentDownloader() {
        _closed = true;
        _empty.notify_all();
        if (_download_thread.joinable()) {
            _download_thread.join();
        }
    }

    virtual void download_segments(const std::string& brpc_addr,
                                   const std::vector<FileCacheSegmentMeta>& metas) = 0;

    inline static FileCacheSegmentDownloader* downloader {nullptr};

    static FileCacheSegmentDownloader* instance() {
        DCHECK(downloader);
        return downloader;
    }

    void submit_download_task(std::string brpc_addr, std::vector<FileCacheSegmentMeta> metas);

    void polling_download_task();

private:
    std::thread _download_thread;
    std::mutex _mtx;
    std::condition_variable _empty;
    std::condition_variable _full;
    std::list<std::pair<std::string, std::vector<FileCacheSegmentMeta>>> _task_queue;
    std::atomic_bool _closed {false};
    size_t max_size {2};
};

class FileCacheSegmentS3Downloader : FileCacheSegmentDownloader {
public:
    static void create_preheating_s3_downloader() {
        static FileCacheSegmentS3Downloader s3_downloader;
        FileCacheSegmentDownloader::downloader = &s3_downloader;
    }

    FileCacheSegmentS3Downloader();

    void download_segments(const std::string& brpc_addr,
                           const std::vector<FileCacheSegmentMeta>& metas) override;

private:
    inline static size_t S3_TRANSFER_EXECUTOR_POOL_SIZE {8};
    std::shared_ptr<Aws::Utils::Threading::PooledThreadExecutor> _executor;
    std::atomic<size_t> _cur_download_file {0};
};

} // namespace doris::io
