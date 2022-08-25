
// clang-format off
#include "meta_server.h"

#include "common/config.h"
#include "common/logging.h"

#include <iostream>
#include <filesystem>
#include <fstream>
// clang-format on

int main(int argc, char** argv) {
    std::string process_name = "meta_service";
    std::cerr << "process current path: " << std::filesystem::current_path() << std::endl;
    std::string pid_path = "./bin/" + process_name + ".pid";
    std::fstream pidfile(pid_path, std::ios::out);
    if (!pidfile.is_open()) {
        std::cerr << "failed to open pid file " << pid_path << std::endl;
        return -1;
    }
    pidfile << getpid() << std::endl;
    pidfile.close();

    auto conf_file = "./conf/meta_service.conf";
    if (!selectdb::config::init(conf_file, true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }
    if (!selectdb::init_glog(process_name.data())) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }

    selectdb::MetaServer meta_server;
    int ret = meta_server.start();
    if (ret != 0) {
        std::cerr << "failed to start meta server" << std::endl;
    }
    meta_server.join(); // Wait for signals
    return 0;
}
// vim: et ts=4 sw=4 cc=80:
