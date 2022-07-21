#include "cloud/cloud_meta_mgr.h"

#include <brpc/channel.h>
#include <brpc/controller.h>

#include "common/config.h"
#include "gen_cpp/selectdb_cloud.pb.h"

namespace doris {

CloudMetaMgr::CloudMetaMgr(const std::string& endpoint) : _endpoint(endpoint) {}

CloudMetaMgr::~CloudMetaMgr() = default;

Status CloudMetaMgr::open() {
    brpc::ChannelOptions options;
    auto channel = std::make_unique<brpc::Channel>();
    if (channel->Init(_endpoint.c_str(), config::rpc_load_balancer.c_str(), &options) != 0) {
        return Status::InternalError("fail to init brpc channel, encpoint: {}", _endpoint);
    }
    _stub = std::make_unique<selectdb::MetaService_Stub>(
            channel.release(), google::protobuf::Service::STUB_OWNS_CHANNEL);
    return Status::OK();
}

Status CloudMetaMgr::get_tablet_meta(int64_t tablet_id, TabletMetaSharedPtr* tablet_meta) {
    brpc::Controller cntl;
    selectdb::GetTabletRequest req;
    selectdb::GetTabletResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_tablet_id(tablet_id);
    _stub->get_tablet(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return Status::IOError("failed to get tablet meta: {}", cntl.ErrorText());
    }
    if (resp.status().code() != 0) {
        return Status::InternalError("failed to get tablet meta: {}", resp.status().msg());
    }
    *tablet_meta = std::make_shared<TabletMeta>();
    (*tablet_meta)->init_from_pb(resp.tablet_meta());
    return Status::OK();
}

Status CloudMetaMgr::get_rowset_meta(int64_t tablet_id, Version version_range,
                                     std::vector<RowsetMetaSharedPtr>* rs_metas) {
    brpc::Controller cntl;
    selectdb::GetRowsetRequest req;
    selectdb::GetRowsetResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_tablet_id(tablet_id);
    req.set_start_version(version_range.first);
    req.set_end_version(version_range.second);
    _stub->get_rowset(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return Status::IOError("failed to get rowset meta: {}", cntl.ErrorText());
    }
    if (resp.status().code() != 0) {
        return Status::InternalError("failed to get rowset meta: {}", resp.status().msg());
    }
    rs_metas->clear();
    rs_metas->reserve(resp.rowset_meta().size());
    for (const auto& meta_pb : resp.rowset_meta()) {
        auto rs_meta = std::make_shared<RowsetMeta>();
        rs_meta->init_from_pb(meta_pb);
        rs_metas->push_back(std::move(rs_meta));
    }
    return Status::OK();
}

Status CloudMetaMgr::write_rowset_meta(const RowsetMetaSharedPtr& rs_meta, bool is_tmp) {
    brpc::Controller cntl;
    selectdb::CreateRowsetRequest req;
    selectdb::MetaServiceGenericResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    *req.mutable_rowset_meta() = rs_meta->get_rowset_pb();
    req.set_temporary(is_tmp);
    _stub->create_rowset(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return Status::IOError("failed to write rowset meta: {}", cntl.ErrorText());
    }
    if (resp.status().code() != 0) {
        return Status::InternalError("failed to write rowset meta: {}", resp.status().msg());
    }
    return Status::OK();
}

Status CloudMetaMgr::commit_txn(int64_t db_id, int64_t txn_id, bool is_2pc) {
    brpc::Controller cntl;
    selectdb::CommitTxnRequest req;
    selectdb::CommitTxnResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_db_id(db_id);
    req.set_txn_id(txn_id);
    req.set_is_2pc(is_2pc);
    _stub->commit_txn(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return Status::IOError("failed to commit txn: {}", cntl.ErrorText());
    }
    if (resp.status().code() != 0) {
        return Status::InternalError("failed to commit txn: {}", resp.status().msg());
    }
    return Status::OK();
}

Status CloudMetaMgr::abort_txn(int64_t db_id, int64_t txn_id) {
    return Status::OK();
}

Status CloudMetaMgr::precommit_txn(int64_t db_id, int64_t txn_id) {
    brpc::Controller cntl;
    selectdb::PrecommitTxnRequest req;
    selectdb::PrecommitTxnResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_db_id(db_id);
    req.set_txn_id(txn_id);
    _stub->precommit_txn(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return Status::IOError("failed to precommit txn: {}", cntl.ErrorText());
    }
    if (resp.status().code() != 0) {
        return Status::InternalError("failed to precommit txn: {}", resp.status().msg());
    }
    return Status::OK();
}

Status CloudMetaMgr::get_s3_info(const std::string& resource_id,
                                 std::map<std::string, std::string>* s3_info) {
    return Status::OK();
}

} // namespace doris
