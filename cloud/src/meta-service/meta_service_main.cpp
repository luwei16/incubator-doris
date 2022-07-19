
// clang-format off
#include "meta_server.h"
#include "common/config.h"

#include <iostream>
// clang-format on

int main(int argc, char** argv) {
    selectdb::MetaServer meta_server;
    int ret = meta_server.start(selectdb::config::brpc_listen_port);
    if (ret != 0) {
        std::cerr << "failed to start meta server" << std::endl;
    }
    meta_server.join(); // Wait for signals
    return 0;
}
// vim: et ts=4 sw=4 cc=80:
