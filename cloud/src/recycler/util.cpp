#include <glog/logging.h>
#include <signal.h>

#include <atomic>
#include <cstring>
#include <mutex>

static sighandler_t s_prev_sigint_handler = nullptr;
static sighandler_t s_prev_sigterm_handler = nullptr;

static void signal_handler(int signal) {
    LOG(INFO) << "signal_handler capture signal=" << signal;
    // Quit
    if (SIGINT == signal && s_prev_sigint_handler) {
        s_prev_sigint_handler(signal);
    }
    if (SIGTERM == signal && s_prev_sigterm_handler) {
        s_prev_sigterm_handler(signal);
    }
}

static int install_signal(int signo, sighandler_t handler) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(struct sigaction));
    sa.sa_handler = handler;
    sigemptyset(&sa.sa_mask);
    int ret = sigaction(signo, &sa, nullptr);
    if (ret != 0) {
        LOG(ERROR) << "install signal failed, signo=" << signo << ", errno=" << errno
                   << ", err=" << std::strerror(errno);
    }
    return ret;
}

static void init_signals() {
    static std::once_flag flag;
    std::call_once(flag, [] {
        if (install_signal(SIGINT, signal_handler) != 0) {
            exit(-1);
        }
        if (install_signal(SIGTERM, signal_handler) != 0) {
            exit(-1);
        }
    });
}
