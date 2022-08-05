#pragma once

#include <vector>

#include "common/status.h"
#include "olap/tablet_meta.h"
#include "runtime/stream_load/stream_load_context.h"
#include "util/s3_util.h"

namespace doris::cloud {

class MetaMgr {
public:
    virtual ~MetaMgr() = default;

    virtual Status open() { return Status::OK(); }

    virtual Status get_tablet_meta(int64_t tablet_id, TabletMetaSharedPtr* tablet_meta) = 0;

    virtual Status get_rowset_meta(int64_t tablet_id, Version version_range,
                                   std::vector<RowsetMetaSharedPtr>* rs_metas) = 0;

    virtual Status write_tablet_meta(const TabletMetaSharedPtr& tablet_meta) = 0;

    virtual Status prepare_rowset(const RowsetMetaSharedPtr& rs_meta, bool is_tmp) = 0;

    virtual Status commit_rowset(const RowsetMetaSharedPtr& rs_meta, bool is_tmp) = 0;

    virtual Status commit_txn(StreamLoadContext* ctx, bool is_2pc) = 0;

    virtual Status abort_txn(StreamLoadContext* ctx) = 0;

    virtual Status precommit_txn(StreamLoadContext* ctx) = 0;

    virtual Status get_s3_info(const std::string& resource_id, S3Conf* s3_info) = 0;
};

} // namespace doris::cloud
