
// clang-format off
#include "meta_server.h"

#include "common/config.h"
#include "meta-service/meta_service.h"
#include "resource-manager/resource_manager.h"

#include "brpc/server.h"

#include <memory>
// clang-format on

namespace selectdb {

MetaServer::MetaServer() {
    server_.reset(new brpc::Server());
}

int MetaServer::start() {
    txn_kv_ = std::dynamic_pointer_cast<TxnKv>(std::make_shared<FdbTxnKv>());
    if (txn_kv_ == nullptr) {
        LOG(WARNING) << "failed to create txn kv, invalid txnkv type";
        return 1;
    }
    int ret = txn_kv_->init();
    if (ret != 0) {
        LOG(WARNING) << "failed to init txnkv, ret=" << ret;
        return 1;
    }

    auto rc_mgr = std::make_shared<ResourceManager>(txn_kv_);
    ret = rc_mgr->init();
    if (ret != 0) {
        LOG(WARNING) << "failed to init resrouce manager, ret=" << ret;
        return 1;
    }

    // Add service
    auto meta_service = new MetaServiceImpl(txn_kv_, rc_mgr);
    server_->AddService(meta_service, brpc::SERVER_OWNS_SERVICE);
    // start service
    brpc::ServerOptions options;
    if (config::brpc_num_threads != -1) {
        options.num_threads = config::brpc_num_threads;
    }
    int port = selectdb::config::brpc_listen_port;
    if (server_->Start(port, &options) != 0) {
        char buf[64];
        LOG(WARNING) << "failed to start brpc, errno=" << errno
                     << ", errmsg=" << strerror_r(errno, buf, 64) << ", port=" << port;
        return -1;
    }
    return 0;
}

void MetaServer::join() {
    server_->RunUntilAskedToQuit();
    // server_->Stop(1000);
    // server_->Join();
    server_->ClearServices();
}

} // namespace selectdb
// vim: et tw=120 ts=4 sw=4 cc=80:
