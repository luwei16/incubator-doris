#include "meta_server.h"
#include <memory>

// clang-format off
#include "common/config.h"
#include "meta_service.h"

#include "brpc/server.h"
// clang-format on

// brpc
// #include <brpc/channel.h>
// #include <brpc/closure_guard.h>
// #include <brpc/controller.h>
// #include <brpc/protocol.h>
// #include <brpc/reloadable_flags.h>
// #include <brpc/server.h>
// #include <bthread/bthread.h>
// #include <bthread/types.h>
// #include <butil/containers/flat_map.h>
// #include <butil/containers/flat_map_inl.h>
// #include <butil/endpoint.h>
// #include <butil/fd_utility.h>
// #include <butil/macros.h>

namespace selectdb {

MetaServer::MetaServer() {
    server_.reset(new brpc::Server());
}

int MetaServer::init() {
    txn_kv_ = std::dynamic_pointer_cast<TxnKv>(std::make_shared<FdbTxnKv>());
    return txn_kv_ != nullptr;
}

int MetaServer::start(int port) {
    // Add service
    server_->AddService(new MetaServiceImpl(), brpc::SERVER_OWNS_SERVICE);
    // start service
    brpc::ServerOptions options;
    if (config::brpc_num_threads != -1) {
        options.num_threads = config::brpc_num_threads;
    }
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
