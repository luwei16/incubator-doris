#pragma once

#include <vector>

#include "common/status.h"
#include "olap/tablet_meta.h"
#include "runtime/stream_load/stream_load_context.h"
#include "util/s3_util.h"

namespace selectdb {
    class TabletJobInfoPB;
}

namespace doris::cloud {

class MetaMgr {
public:
    virtual ~MetaMgr() = default;

    virtual Status open() { return Status::OK(); }

    virtual Status get_tablet_meta(int64_t tablet_id, TabletMetaSharedPtr* tablet_meta) {
        return Status::NotSupported("not supported");
    }

    virtual Status get_rowset_meta(const TabletMetaSharedPtr& tablet_meta, Version version_range,
                                   std::vector<RowsetMetaSharedPtr>* rs_metas) {
        return Status::NotSupported("not supported");
    }

    virtual Status write_tablet_meta(const TabletMetaSharedPtr& tablet_meta) {
        return Status::NotSupported("not supported");
    }

    virtual Status prepare_rowset(const RowsetMetaSharedPtr& rs_meta, bool is_tmp) {
        return Status::NotSupported("not supported");
    }

    virtual Status commit_rowset(const RowsetMetaSharedPtr& rs_meta, bool is_tmp) {
        return Status::NotSupported("not supported");
    }

    virtual Status commit_txn(StreamLoadContext* ctx, bool is_2pc) {
        return Status::NotSupported("not supported");
    }

    virtual Status abort_txn(StreamLoadContext* ctx) {
        return Status::NotSupported("not supported");
    }

    virtual Status precommit_txn(StreamLoadContext* ctx) {
        return Status::NotSupported("not supported");
    }

    virtual Status get_s3_info(std::vector<std::tuple<std::string, S3Conf>>* s3_infos) {
        return Status::NotSupported("not supported");
    }

    virtual Status prepare_tablet_job(const selectdb::TabletJobInfoPB& job) {
        return Status::NotSupported("not supported");
    }

    virtual Status commit_tablet_job(const selectdb::TabletJobInfoPB& job) {
        return Status::NotSupported("not supported");
    }

    virtual Status abort_tablet_job(const selectdb::TabletJobInfoPB& job) {
        return Status::NotSupported("not supported");
    }
};

} // namespace doris::cloud
