#pragma once

#include "cloud/meta_mgr.h"

namespace selectdb {
class MetaService_Stub;
}

namespace doris {

class CloudMetaMgr final : public MetaMgr {
public:
    CloudMetaMgr(const std::string& endpoint);

    ~CloudMetaMgr() override;

    Status open() override;

    Status get_tablet_meta(int64_t tablet_id, TabletMetaSharedPtr* tablet_meta) override;

    Status get_rowset_meta(int64_t tablet_id, Version version_range,
                           std::vector<RowsetMetaSharedPtr>* rs_metas) override;

    Status write_rowset_meta(const RowsetMetaSharedPtr& rs_meta, bool is_tmp) override;

    Status commit_txn(int64_t db_id, int64_t txn_id, bool is_2pc) override;

    Status abort_txn(int64_t db_id, int64_t txn_id) override;

    Status precommit_txn(int64_t db_id, int64_t txn_id) override;

    Status get_s3_info(const std::string& resource_id,
                       std::map<std::string, std::string>* s3_info) override;

private:
    std::string _endpoint;

    std::unique_ptr<selectdb::MetaService_Stub> _stub;
};

} // namespace doris
