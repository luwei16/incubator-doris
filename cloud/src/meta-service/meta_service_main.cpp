
#include <iostream>

#include "meta_server.h"

int main(int argc, char** argv) {
    selectdb::MetaServer meta_server;
    int ret = meta_server.start(7777);
    if (ret != 0) {
        std::cerr << "failed to start meta server" << std::endl;
    }
    meta_server.join(); // Wait for signals
    return 0;
}
// vim: et ts=4 sw=4 cc=80:
