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

#include "io/fs/s3_file_writer.h"

#include <aws/transfer/TransferManager.h>

#include <mutex>
#include <utility>

#include "common/config.h"
#include "common/status.h"
#include "io/cloud/tmp_file_mgr.h"
#include "io/fs/local_file_system.h"
#include "io/fs/s3_file_system.h"

namespace doris {
namespace io {

S3FileWriter::S3FileWriter(Path path, std::string key, std::string bucket, S3FileSystem* fs)
        : FileWriter(std::move(path)), _fs(fs), _bucket(std::move(bucket)), _key(std::move(key)) {}

S3FileWriter::~S3FileWriter() {
    if (!_closed) {
        abort();
    }
}

Status S3FileWriter::open() {
    VLOG_DEBUG << "S3FileWriter::open, path: " << _path.native();
    auto tmp_file_name = _key;
    std::replace(tmp_file_name.begin(), tmp_file_name.end(), '/', '_');
    auto st = io::global_local_filesystem()->create_file(
            TmpFileMgr::instance()->get_tmp_file_dir(tmp_file_name) / tmp_file_name,
            &_tmp_file_writer);
    if (!st.ok()) {
        return Status::IOError("failed to create tmp file: {}", st.to_string());
    }
    _closed = false;
    return Status::OK();
}

Status S3FileWriter::abort() {
    if (_closed) {
        return Status::OK();
    }
    VLOG_DEBUG << "S3FileWriter::abort, path: " << _path.native();
    _closed = true;
    if (_handle) {
        _handle->Cancel();
    }
    return Status::OK();
}

Status S3FileWriter::close() {
    if (_closed) {
        return Status::OK();
    }
    VLOG_DEBUG << "S3FileWriter::close, path: " << _path.native();
    DCHECK(_handle);
    _closed = true;

    {
        SCOPED_ATTACH_TASK(ExecEnv::GetInstance()->orphan_mem_tracker());
        _handle->WaitUntilFinished();
        if (_handle->GetStatus() != Aws::Transfer::TransferStatus::COMPLETED) {
            return Status::IOError("failed to upload {}: {}", _path.native(),
                                   _handle->GetLastError().GetMessage());
        }
    }
    // TODO(cyx): check data correctness
    return Status::OK();
}

Status S3FileWriter::append(const Slice& data) {
    DCHECK(!_closed);
    DCHECK(_handle == nullptr);
    return _tmp_file_writer->append(data);
}

Status S3FileWriter::appendv(const Slice* data, size_t data_cnt) {
    DCHECK(!_closed);
    DCHECK(_handle == nullptr);
    return _tmp_file_writer->appendv(data, data_cnt);
}

Status S3FileWriter::write_at(size_t offset, const Slice& data) {
    DCHECK(!_closed);
    DCHECK(_handle == nullptr);
    return _tmp_file_writer->write_at(offset, data);
}

Status S3FileWriter::finalize() {
    DCHECK(!_closed);
    DCHECK(_handle == nullptr);
    auto client = _fs->get_client();
    if (!client) {
        return Status::InternalError("init s3 client error");
    }
    RETURN_IF_ERROR(_tmp_file_writer->close());
    {
        SCOPED_ATTACH_TASK(ExecEnv::GetInstance()->orphan_mem_tracker());
        auto tmp_file_mgr = TmpFileMgr::instance();
        bool is_async_upload = tmp_file_mgr->check_if_has_enough_space_to_async_upload(
                _tmp_file_writer->path(), _tmp_file_writer->bytes_appended());

        auto upload_start = std::chrono::steady_clock::now();

        auto upload_callback =
                [tmp_file_mgr, path = _tmp_file_writer->path(),
                 size = _tmp_file_writer->bytes_appended(), is_async_upload = is_async_upload,
                 upload_start = upload_start, upload_speed = _upload_speed_bytes_s](
                        const Aws::Transfer::TransferManager*,
                        const std::shared_ptr<const Aws::Transfer::TransferHandle>& handle) {
                    if (handle->GetStatus() != Aws::Transfer::TransferStatus::COMPLETED ||
                        !tmp_file_mgr->insert_tmp_file(path, size)) {
                        global_local_filesystem()->delete_file(path);
                    }
                    tmp_file_mgr->upload_complete(path, size, is_async_upload);
                    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                                            std::chrono::steady_clock::now() - upload_start)
                                            .count();
                    *upload_speed = static_cast<double>(size) /
                                    (static_cast<double>(duration) / 1000 / 1000);
                };
        Aws::Transfer::TransferManagerConfiguration transfer_config(_fs->_executor.get());
        transfer_config.s3Client = client;
        auto transfer_manager = Aws::Transfer::TransferManager::Create(transfer_config);
        _handle = transfer_manager->UploadFile(_tmp_file_writer->path().native(), _bucket, _key,
                                               "text/plain", Aws::Map<Aws::String, Aws::String>(),
                                               nullptr, std::move(upload_callback));
        if (!is_async_upload) {
            LOG(INFO) << "The current upload files size is too larger, change to sync upload";
            return close();
        }
    }
    return Status::OK();
}

} // namespace io
} // namespace doris
