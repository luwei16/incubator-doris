#pragma once

#include <vector>

#include "common/status.h"
#include "olap/tablet_meta.h"

namespace doris::cloud {

class MetaMgr {
public:
    virtual ~MetaMgr() = default;

    virtual Status open() { return Status::OK(); }

    virtual Status get_tablet_meta(int64_t tablet_id, TabletMetaSharedPtr* tablet_meta) = 0;

    virtual Status get_rowset_meta(int64_t tablet_id, Version version_range,
                                   std::vector<RowsetMetaSharedPtr>* rs_metas) = 0;

    virtual Status write_tablet_meta(const TabletMetaSharedPtr& tablet_meta) = 0;

    virtual Status write_rowset_meta(const RowsetMetaSharedPtr& rs_meta, bool is_tmp) = 0;

    virtual Status commit_txn(int64_t db_id, int64_t txn_id, bool is_2pc) = 0;

    virtual Status abort_txn(int64_t db_id, int64_t txn_id) = 0;

    virtual Status precommit_txn(int64_t db_id, int64_t txn_id) = 0;

    virtual Status get_s3_info(const std::string& resource_id,
                               std::map<std::string, std::string>* s3_info) = 0;
};

} // namespace doris::cloud
