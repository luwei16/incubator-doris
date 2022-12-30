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

#pragma once

#include <atomic>
#include <chrono>
#include <list>
#include <memory>
#include <unordered_set>

#include "common/config.h"
#include "gutil/int128.h"
#include "cloud/io/file_writer.h"
#include "cloud/io/s3_file_system.h"

namespace Aws::Transfer {
class TransferHandle;
}

namespace doris {
namespace io {

class S3FileSystem;
class TmpFileMgr;
class S3FileWriter final : public FileWriter {
public:
    S3FileWriter(Path path, std::string key, std::string bucket, std::shared_ptr<S3FileSystem> fs);
    ~S3FileWriter() override;

    Status open() override;

    Status close(bool sync = true) override;

    Status abort() override;

    Status append(const Slice& data) override;

    Status appendv(const Slice* data, size_t data_cnt) override;

    Status write_at(size_t offset, const Slice& data) override;

    Status finalize() override;

    size_t bytes_appended() const override { return _tmp_file_writer->bytes_appended(); }

    FileSystemSPtr fs() const override { return _fs; }
    int64_t upload_cost_ms() const { return *_upload_cost_ms; }

    const Path& path() const override { return _path; }

private:
    Path _path;
    std::shared_ptr<S3FileSystem> _fs;

    std::string _bucket;
    std::string _key;
    bool _closed = true;

    FileWriterPtr _tmp_file_writer;
    std::shared_ptr<Aws::Transfer::TransferHandle> _handle;

    std::shared_ptr<int64_t> _upload_cost_ms;
};

} // namespace io
} // namespace doris
