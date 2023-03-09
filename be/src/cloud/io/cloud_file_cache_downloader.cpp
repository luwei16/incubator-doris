#include "cloud/io/cloud_file_cache_downloader.h"

#include <aws/transfer/TransferHandle.h>
#include <aws/transfer/TransferManager.h>
#include <fmt/core.h>
#include <gen_cpp/internal_service.pb.h>

#include "cloud/io/cloud_file_cache.h"
#include "cloud/io/cloud_file_cache_factory.h"
#include "cloud/io/cloud_file_segment.h"
#include "cloud/io/s3_file_system.h"
#include "cloud/utils.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/tablet.h"

namespace doris::io {

void FileCacheSegmentDownloader::submit_download_task(std::string brpc_addr,
                                                      std::vector<FileCacheSegmentMeta> metas) {
    std::unique_lock lock(_mtx);
    if (_task_queue.size() == max_size) {
        _full.wait(lock, [this]() { return _task_queue.size() < max_size; });
    }
    _task_queue.push_back(std::make_pair(std::move(brpc_addr), std::move(metas)));
    _empty.notify_all();
}

void FileCacheSegmentDownloader::polling_download_task() {
    while (true) {
        std::unique_lock lock(_mtx);
        if (_task_queue.empty()) {
            _empty.wait(lock, [this]() { return !_task_queue.empty() || _closed; });
        }
        if (_closed) {
            break;
        }
        download_segments(_task_queue.front().first, _task_queue.front().second);
        _task_queue.pop_front();
        _full.notify_all();
    }
}

void FileCacheSegmentS3Downloader::download_segments(
        const std::string& brpc_addr [[maybe_unused]],
        const std::vector<FileCacheSegmentMeta>& metas) {
    std::for_each(metas.cbegin(), metas.cend(), [this](const FileCacheSegmentMeta& meta) {
        TabletSharedPtr tablet;
        cloud::tablet_mgr()->get_tablet(meta.tablet_id(), &tablet);
        auto id_to_rowset_meta_map = tablet->tablet_meta()->snapshot_rs_metas();
        if (auto iter = id_to_rowset_meta_map.find(meta.rowset_id());
            iter != id_to_rowset_meta_map.end()) {
            IFileCache::Key cache_key = IFileCache::hash(meta.file_name());
            CloudFileCachePtr cache = FileCacheFactory::instance().get_by_path(cache_key);
            CacheContext context;
            switch (meta.cache_type()) {
                case FileCacheType::TTL:
                    context.cache_type = CacheType::TTL;
                    break;
                case FileCacheType::INDEX:
                    context.cache_type = CacheType::INDEX;
                    break;
                default:
                    context.cache_type = CacheType::NORMAL;
            }
            context.expiration_time = meta.expiration_time();
            FileSegmentsHolder holder =
                    cache->get_or_set(cache_key, meta.offset(), meta.size(), context);
            DCHECK(holder.file_segments.size() == 1);
            auto file_segment = holder.file_segments.front();
            if (file_segment->state() == FileSegment::State::EMPTY &&
                file_segment->get_or_set_downloader() == FileSegment::get_caller_id()) {
                S3FileSystem* s3_file_system =
                        dynamic_cast<S3FileSystem*>(iter->second->fs().get());
                size_t cur_download_size, next_download_size;
                do {
                    cur_download_size = _cur_download_file;
                    next_download_size = cur_download_size + 1;
                } while (cur_download_size >= S3_TRANSFER_EXECUTOR_POOL_SIZE ||
                         !_cur_download_file.compare_exchange_strong(cur_download_size,
                                                                     next_download_size));
                auto transfer_manager = s3_file_system->get_transfer_manager();
                if (!transfer_manager) {
                    return;
                }
                auto download_callback = [this, file_segment,
                                          meta](const Aws::Transfer::TransferHandle* handle) {
                    if (handle->GetStatus() == Aws::Transfer::TransferStatus::NOT_STARTED ||
                        handle->GetStatus() == Aws::Transfer::TransferStatus::IN_PROGRESS) {
                        return; // not finish
                    }
                    if (handle->GetStatus() == Aws::Transfer::TransferStatus::COMPLETED) {
                        file_segment->finalize_write();
                    } else {
                        LOG(WARNING) << "s3 download error " << handle->GetStatus();
                    }
                    _cur_download_file--;
                };
                std::string download_file = file_segment->get_path_in_local_cache();
                auto createFileFn = [=]() {
                    return Aws::New<Aws::FStream>(meta.file_name().c_str(), download_file.c_str(),
                                                  std::ios_base::out | std::ios_base::in |
                                                          std::ios_base::binary |
                                                          std::ios_base::trunc);
                };
                transfer_manager->DownloadFile(
                        s3_file_system->s3_conf().bucket,
                        s3_file_system->get_key(BetaRowset::remote_segment_path(
                                meta.tablet_id(), meta.rowset_id(), meta.segment_id())),
                        meta.offset(), meta.size(), std::move(createFileFn),
                        Aws::Transfer::DownloadConfiguration(), download_file, nullptr,
                        download_callback);
            }
        }
    });
}

FileCacheSegmentS3Downloader::FileCacheSegmentS3Downloader() {
    _executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(
            "FileCacheSegmentS3Downloader", S3_TRANSFER_EXECUTOR_POOL_SIZE);
}

} // namespace doris::io
