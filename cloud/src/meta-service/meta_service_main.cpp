
// clang-format off
#include "meta_server.h"

#include "common/config.h"
#include "common/logging.h"
#include "recycler/recycler.h"

#include <unistd.h> // ::lockf
#include <fcntl.h> // ::open
#include <functional>
#include <iostream>
#include <filesystem>
#include <fstream>
#include <mutex>
// clang-format on

/**
 * Generates a pidfile with given process name
 *
 * @return an fd holder with auto-storage-lifecycle
 */
std::shared_ptr<int> gen_pidfile(const std::string& process_name) {
    std::cerr << "process current path: " << std::filesystem::current_path() << std::endl;
    std::string pid_path = "./bin/" + process_name + ".pid";
    int fd = ::open(pid_path.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    // clang-format-off
    std::shared_ptr<int> holder(&fd, // Just pretend to need an address of int
            [fd, pid_path](...) {    // The real fd is captured
                if (fd <= 0) { return; }
                [[maybe_unused]] auto x = ::lockf(fd, F_UNLCK, 0);
                ::close(fd);
                std::error_code ec; std::filesystem::remove(pid_path, ec);
            });
    // clang-format-on
    if (::lockf(fd, F_TLOCK, 0) != 0) {
        std::cerr << "failed to lock pidfile=" << pid_path << " fd=" << fd << std::endl;
        return nullptr;
    }
    std::fstream pidfile(pid_path, std::ios::out);
    if (!pidfile.is_open()) {
        std::cerr << "failed to open pid file " << pid_path << std::endl;
        return nullptr;
    }
    pidfile << getpid() << std::endl;
    pidfile.close();
    return holder;
}

// TODO(gavin): support daemon mode
// must be called before pidfile generation and any network resource
// initializaiton, <https://man7.org/linux/man-pages/man3/daemon.3.html>
// void daemonize(1, 1); // Maybe nohup will do?

int main(int argc, char** argv) {
    std::string process_name = "meta_service";
    if (argc > 1) {
        if (strcmp(argv[1], "--recycler") == 0) {
            process_name = "recycler";
        }
    }

    auto fd_holder = gen_pidfile(process_name);
    if (fd_holder == nullptr) {
        return -1;
    }

    auto conf_file = "./conf/meta_service.conf";
    if (!selectdb::config::init(conf_file, true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }
    if (!selectdb::init_glog(process_name.data())) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }

    if (process_name == "recycler") {
        selectdb::Recycler recycler;
        int ret = recycler.start();
        if (ret != 0) {
            std::cerr << "failed to start recycler" << std::endl;
        }
        return 0;
    }

    selectdb::MetaServer meta_server;
    int ret = meta_server.start();
    if (ret != 0) {
        std::cerr << "failed to start meta server" << std::endl;
        return ret;
    }
    meta_server.join(); // Wait for signals
    return 0;
}
// vim: et ts=4 sw=4 cc=80:
