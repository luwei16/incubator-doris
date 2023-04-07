#include "recycler/recycler.h"

#include <butil/strings/string_split.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/selectdb_cloud.pb.h>
#include <signal.h>

#include <atomic>
#include <chrono>
#include <string>
#include <string_view>
#ifdef UNIT_TEST
#include "../test/mock_accessor.h"
#endif
#include "common/config.h"
#include "common/encryption_util.h"
#include "common/logging.h"
#include "common/simple_thread_pool.h"
#include "common/sync_point.h"
#include "common/util.h"
#include "meta-service/keys.h"
#include "recycler/checker.h"
#include "recycler/recycler_service.h"

namespace selectdb {

static std::atomic_bool s_is_working = false;
static std::mutex s_working_mtx;
static std::condition_variable s_working_cond;

static bool is_working() {
#ifdef UNIT_TEST
    return true;
#endif
    return s_is_working.load(std::memory_order_acquire);
}

void Recycler::InstanceFilter::reset(const std::string& whitelist, const std::string& blacklist) {
    blacklist_.clear();
    whitelist_.clear();
    if (!whitelist.empty()) {
        std::vector<std::string> list;
        butil::SplitString(whitelist, ',', &list);
        for (auto& str : list) {
            whitelist_.insert(std::move(str));
        }
    } else {
        std::vector<std::string> list;
        butil::SplitString(blacklist, ',', &list);
        for (auto& str : list) {
            blacklist_.insert(std::move(str));
        }
    }
}

bool Recycler::InstanceFilter::filter_out(const std::string& instance_id) const {
    if (whitelist_.empty()) {
        return blacklist_.count(instance_id);
    }
    return !whitelist_.count(instance_id);
}

static void signal_handler(int signal) {
    LOG(INFO) << "signal_handler capture signal=" << signal;
    if (signal == SIGINT || signal == SIGTERM) {
        s_is_working = false;
        s_working_cond.notify_all();
    }
}

static int install_signal(int signo, void (*handler)(int)) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(struct sigaction));
    sa.sa_handler = handler;
    sigemptyset(&sa.sa_mask);
    int ret = sigaction(signo, &sa, nullptr);
    if (ret != 0) {
        char buf[64];
        LOG(ERROR) << "install signal failed, signo=" << signo << ", errno=" << errno
                   << ", err=" << strerror_r(errno, buf, sizeof(buf));
    }
    return ret;
}

static void init_signals() {
    int ret = install_signal(SIGINT, signal_handler);
    if (ret < 0) {
        exit(-1);
    }
    ret = install_signal(SIGTERM, signal_handler);
    if (ret < 0) {
        exit(-1);
    }
}

std::string segment_path(int64_t tablet_id, const std::string& rowset_id, int64_t segment_id) {
    return fmt::format("data/{}/{}_{}.dat", tablet_id, rowset_id, segment_id);
}

std::string inverted_index_path(int64_t tablet_id, const std::string& rowset_id, int64_t segment_id,
                                int64_t index_id) {
    return fmt::format("data/{}/{}_{}_{}.idx", tablet_id, rowset_id, segment_id, index_id);
}

std::string rowset_path_prefix(int64_t tablet_id, const std::string& rowset_id) {
    return fmt::format("data/{}/{}_", tablet_id, rowset_id);
}

std::string tablet_path_prefix(int64_t tablet_id) {
    return fmt::format("data/{}/", tablet_id);
}

// return 0 for success get a key, 1 for key not found, negative for error
[[maybe_unused]] static int txn_get(TxnKv* txn_kv, std::string_view key, std::string& val) {
    std::unique_ptr<Transaction> txn;
    int ret = txn_kv->create_txn(&txn);
    if (ret != 0) {
        return -1;
    }
    return txn->get(key, &val);
}

// 0 for success, negative for error
static int txn_get(TxnKv* txn_kv, std::string_view begin, std::string_view end,
                   std::unique_ptr<RangeGetIterator>& it) {
    std::unique_ptr<Transaction> txn;
    int ret = txn_kv->create_txn(&txn);
    if (ret != 0) {
        return -1;
    }
    return txn->get(begin, end, &it);
}

// return 0 for success otherwise error
static int txn_remove(TxnKv* txn_kv, std::vector<std::string_view> keys) {
    std::unique_ptr<Transaction> txn;
    int ret = txn_kv->create_txn(&txn);
    if (ret != 0) {
        return -1;
    }
    for (auto k : keys) {
        txn->remove(k);
    }
    return txn->commit();
}

// return 0 for success otherwise error
static int txn_remove(TxnKv* txn_kv, std::vector<std::string> keys) {
    std::unique_ptr<Transaction> txn;
    int ret = txn_kv->create_txn(&txn);
    if (ret != 0) {
        return -1;
    }
    for (auto& k : keys) {
        txn->remove(k);
    }
    return txn->commit();
}

// return 0 for success otherwise error
[[maybe_unused]] static int txn_remove(TxnKv* txn_kv, std::string_view begin,
                                       std::string_view end) {
    std::unique_ptr<Transaction> txn;
    int ret = txn_kv->create_txn(&txn);
    if (ret != 0) {
        return -1;
    }
    txn->remove(begin, end);
    return txn->commit();
}

Recycler::Recycler() = default;

Recycler::~Recycler() = default;

std::vector<InstanceInfoPB> Recycler::get_instances() {
    std::vector<InstanceInfoPB> instances;
    InstanceKeyInfo key0_info {""};
    InstanceKeyInfo key1_info {"\xff"}; // instance id are human readable strings
    std::string key0;
    std::string key1;
    instance_key(key0_info, &key0);
    instance_key(key1_info, &key1);

    std::unique_ptr<Transaction> txn;
    int ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        LOG(INFO) << "failed to init txn, ret=" << ret;
        return instances;
    }

    std::unique_ptr<RangeGetIterator> it;
    do {
        ret = txn->get(key0, key1, &it);
        if (ret != 0) {
            LOG(WARNING) << "internal error, failed to get instance, ret=" << ret;
            return instances;
        }

        while (it->has_next()) {
            auto [k, v] = it->next();
            if (!it->has_next()) key0 = k;

            InstanceInfoPB instance_info;
            if (!instance_info.ParseFromArray(v.data(), v.size())) {
                LOG_WARNING("malformed instance info").tag("key", hex(k));
                return instances;
            }

            LOG(INFO) << "get an instance, instance_id=" << instance_info.instance_id();
            instances.push_back(std::move(instance_info));
        }
        key0.push_back('\x00'); // Update to next smallest key for iteration
    } while (it->more());

    return instances;
}

void Recycler::add_pending_instances(std::vector<InstanceInfoPB> instances) {
    if (instances.empty()) {
        return;
    }
    std::lock_guard lock(pending_instance_mtx_);
    for (auto& instance : instances) {
        if (instance_filter_.filter_out(instance.instance_id())) continue;
        auto [_, success] = pending_instance_set_.insert(instance.instance_id());
        // skip instance already in pending queue
        if (success) {
            pending_instance_queue_.push_back(std::move(instance));
        }
    }
    pending_instance_cond_.notify_all();
}

void Recycler::instance_scanner_callback() {
    while (is_working()) {
        auto instances = get_instances();
        // TODO(plat1ko): delete job recycle kv of non-existent instances
        add_pending_instances(std::move(instances));
        {
            std::unique_lock lock(s_working_mtx);
            s_working_cond.wait_for(lock, std::chrono::seconds(config::recycle_interval_seconds),
                                    []() { return !is_working(); });
        }
    }
}

bool Recycler::prepare_instance_recycle_job(const std::string& instance_id) {
    JobRecycleKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    job_recycle_key(key_info, &key);
    std::unique_ptr<Transaction> txn;
    int ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        LOG(WARNING) << "failed to create txn";
        return false;
    }
    ret = txn->get(key, &val);
    if (ret < 0) {
        LOG(WARNING) << "failed to get kv";
        return false;
    }

    using namespace std::chrono;
    auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    JobRecyclePB job_info;

    auto is_expired = [&]() {
        if (!job_info.ParseFromString(val)) {
            LOG(WARNING) << "failed to parse JobRecyclePB";
            // if failed to parse, just recycle it.
            return true;
        }
        DCHECK(job_info.instance_id() == instance_id);
        bool lease_expired =
                job_info.status() == JobRecyclePB::BUSY && job_info.expiration_time_ms() < now;
        bool finish_expired =
                job_info.status() == JobRecyclePB::IDLE &&
                now - job_info.last_finish_time_ms() > config::recycle_interval_seconds * 1000;
        return lease_expired || finish_expired;
    };
    if (ret == 1 || is_expired()) {
        job_info.set_status(JobRecyclePB::BUSY);
        job_info.set_instance_id(instance_id);
        job_info.set_ip_port(ip_port_);
        job_info.set_ctime_ms(now);
        job_info.set_expiration_time_ms(now + config::recycle_job_lease_expired_ms);
        job_info.set_last_finish_time_ms(-1);
        val = job_info.SerializeAsString();
        txn->put(key, val);
        ret = txn->commit();
        if (ret != 0) {
            LOG(WARNING) << "failed to commit";
            return false;
        }
        LOG(INFO) << "host: " << ip_port_ << " start recycle job, instance_id: " << instance_id;
        return true;
    }
    return false;
}

void Recycler::finish_instance_recycle_job(const std::string& instance_id) {
    JobRecycleKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    job_recycle_key(key_info, &key);
    int retry_times = 3;
    do {
        std::unique_ptr<Transaction> txn;
        int ret = txn_kv_->create_txn(&txn);
        if (ret != 0) {
            LOG(WARNING) << "failed to create txn";
            return;
        }
        ret = txn->get(key, &val);
        if (ret < 0 || ret == 1) {
            LOG(WARNING) << "failed to get kv";
            return;
        }

        using namespace std::chrono;
        auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        JobRecyclePB job_info;
        if (!job_info.ParseFromString(val)) {
            LOG(WARNING) << "failed to parse JobRecyclePB";
            return;
        }
        DCHECK(job_info.instance_id() == instance_id);
        if (job_info.ip_port() != ip_port_) {
            LOG(WARNING) << "recycle job of instance_id: " << instance_id
                         << " is doing at other machine: " << job_info.ip_port();
            return;
        }
        if (job_info.status() != JobRecyclePB::BUSY) {
            LOG(WARNING) << "recycle job of instance_id: " << instance_id << " is not busy";
            return;
        }
        job_info.set_status(JobRecyclePB::IDLE);
        job_info.set_instance_id(instance_id);
        job_info.set_last_finish_time_ms(now);
        val = job_info.SerializeAsString();
        txn->put(key, val);
        ret = txn->commit();
        if (ret == 0) {
            LOG(INFO) << "succ to commit to finish recycle job, instance_id: " << instance_id;
            return;
        }
        // maybe conflict with the commit of the leased thread
        LOG(WARNING) << "failed to commit to finish recycle job, instance_id: " << instance_id
                     << " try it again";
    } while (retry_times--);
    LOG(WARNING) << "finally failed to commit to finish recycle job, instance_id: " << instance_id;
}

void Recycler::recycle_callback() {
    while (is_working()) {
        InstanceInfoPB instance;
        {
            std::unique_lock lock(pending_instance_mtx_);
            pending_instance_cond_.wait(
                    lock, [&]() { return !pending_instance_queue_.empty() || !is_working(); });
            if (!is_working()) {
                return;
            }
            instance = std::move(pending_instance_queue_.front());
            pending_instance_queue_.pop_front();
            pending_instance_set_.erase(instance.instance_id());
        }
        {
            std::lock_guard lock(recycling_instance_set_mtx_);
            auto [_, success] = recycling_instance_set_.insert(instance.instance_id());
            if (!success) {
                // skip instance in recycling
                continue;
            }
            if (!prepare_instance_recycle_job(instance.instance_id())) {
                recycling_instance_set_.erase(instance.instance_id());
                continue;
            }
        }

        auto& instance_id = instance.instance_id();
        if (instance.obj_info().empty()) {
            LOG_WARNING("instance has no object store info").tag("instance_id", instance_id);
            continue;
        }
        auto instance_recycler = std::make_unique<InstanceRecycler>(txn_kv_, instance);
        if (instance_recycler->init() != 0) {
            LOG(WARNING) << "failed to init instance recycler, instance_id=" << instance_id;
            continue;
        }
        if (instance.status() == InstanceInfoPB::DELETED) {
            instance_recycler->recycle_instance();
        } else if (instance.status() == InstanceInfoPB::NORMAL) {
            LOG_INFO("begin to recycle instance").tag("instance_id", instance_id);
            instance_recycler->recycle_indexes();
            instance_recycler->recycle_partitions();
            instance_recycler->recycle_tmp_rowsets();
            instance_recycler->recycle_rowsets();
            instance_recycler->abort_timeout_txn();
            instance_recycler->recycle_expired_txn_label();
            instance_recycler->recycle_copy_jobs();
            instance_recycler->recycle_stage();
            instance_recycler->recycle_expired_stage_objects();
        } else {
            LOG(WARNING) << "unknown instance status: " << instance.status()
                         << " instance_id=" << instance_id;
        }
        {
            std::lock_guard lock(recycling_instance_set_mtx_);
            recycling_instance_set_.erase(instance_id);
        }
        finish_instance_recycle_job(instance_id);
        LOG_INFO("finish recycle instance").tag("instance_id", instance_id);
    }
}

void Recycler::lease_instance_recycle_job(const std::string& instance_id) {
    JobRecycleKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    job_recycle_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    int ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        LOG(WARNING) << "failed to create txn";
        return;
    }
    ret = txn->get(key, &val);
    if (ret < 0 || ret == 1) {
        LOG(WARNING) << "failed to get kv";
        return;
    }

    using namespace std::chrono;
    auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    JobRecyclePB job_info;
    if (!job_info.ParseFromString(val)) {
        LOG(WARNING) << "failed to parse JobRecyclePB";
        return;
    }
    DCHECK(job_info.instance_id() == instance_id);
    if (job_info.ip_port() != ip_port_) {
        LOG(WARNING) << "recycle job of instance_id: " << instance_id
                     << " is doing at other machine: " << job_info.ip_port();
        // Todo: abort the recycle of this instance_id
        return;
    }
    if (job_info.status() != JobRecyclePB::BUSY) {
        LOG(WARNING) << "recycle job of instance_id: " << instance_id << " is not busy";
        return;
    }
    job_info.set_expiration_time_ms(now + config::recycle_job_lease_expired_ms);
    val = job_info.SerializeAsString();
    txn->put(key, val);
    ret = txn->commit();
    if (ret != 0) {
        LOG(WARNING) << "failed to commit, failed to lease recycle job of instance_id: "
                     << instance_id;
    }
}

void Recycler::do_lease() {
    while (is_working()) {
        std::unordered_set<std::string> instance_set;
        {
            std::lock_guard lock(recycling_instance_set_mtx_);
            instance_set = recycling_instance_set_;
        }
        for (const auto& instance_id : instance_set) {
            lease_instance_recycle_job(instance_id);
        }
        {
            std::unique_lock lock(s_working_mtx);
            s_working_cond.wait_for(
                    lock, std::chrono::milliseconds(config::recycle_job_lease_expired_ms / 3),
                    []() { return !is_working(); });
        }
    }
}

int Recycler::start(bool with_brpc) {
    init_signals(); // for graceful exit

    instance_filter_.reset(config::recycle_whitelist, config::recycle_blacklist);
    int ret = 0;
    txn_kv_ = std::make_shared<FdbTxnKv>();
    LOG(INFO) << "begin to init txn kv";
    ret = txn_kv_->init();
    if (ret != 0) {
        LOG(WARNING) << "failed to init txnkv, ret=" << ret;
        return -1;
    }
    LOG(INFO) << "successfully init txn kv";

    if (init_global_encryption_key_info_map(txn_kv_) != 0) {
        LOG(WARNING) << "failed to init global encryption key map";
        return -1;
    }

    // Whether brpc server is enabled or not, still need `ip_port_` to identify a recycler process in lease mechanism
    ip_port_ = std::string(butil::my_ip_cstr()) + ":" + std::to_string(config::brpc_listen_port);
    if (with_brpc) {
        server_ = std::make_unique<brpc::Server>();
        // Add service
        auto recycler_service = new RecyclerServiceImpl(txn_kv_, this);
        server_->AddService(recycler_service, brpc::SERVER_OWNS_SERVICE);
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
        LOG(INFO) << "successfully started brpc listening on port=" << port;
    }
    if (config::enable_checker) {
        checker_ = std::make_unique<Checker>(txn_kv_);
        ret = checker_->start();
        if (ret != 0) {
            LOG(WARNING) << "failed to init checker, ret=" << ret;
            return -1;
        }
        LOG(INFO) << "successfully init checker";
    }
    s_is_working = true;

    workers_.push_back(std::thread(std::bind(&Recycler::instance_scanner_callback, this)));
    for (int i = 0; i < config::recycle_concurrency; ++i) {
        workers_.push_back(std::thread(std::bind(&Recycler::recycle_callback, this)));
    }

    workers_.push_back(std::thread(std::mem_fn(&Recycler::do_lease), this));
    return 0;
}

void Recycler::join() {
    if (server_) { // with brpc
        server_->RunUntilAskedToQuit();
        server_->ClearServices();
    }
    {
        std::unique_lock lock(s_working_mtx);
        s_working_cond.wait(lock, []() { return !is_working(); });
    }
    if (checker_) { // enable checker
        checker_->stop();
    }
    pending_instance_cond_.notify_all();
    for (auto& w : workers_) {
        w.join();
    }
}

static int decrypt_ak_sk_helper(const std::string& cipher_ak, const std::string& cipher_sk,
                                const EncryptionInfoPB& encryption_info,
                                AkSkPair* plain_ak_sk_pair) {
    std::string key;
    int ret = get_encryption_key_for_ak_sk(encryption_info.key_id(), &key);
    if (ret != 0) {
        LOG(WARNING) << "failed to get encryptionn key version_id: " << encryption_info.key_id();
        return -1;
    }
    ret = decrypt_ak_sk({cipher_ak, cipher_sk}, encryption_info.encryption_method(), key,
                        plain_ak_sk_pair);
    if (ret != 0) {
        LOG(WARNING) << "failed to decrypt";
        return -1;
    }
    return 0;
}

InstanceRecycler::InstanceRecycler(std::shared_ptr<TxnKv> txn_kv, const InstanceInfoPB& instance)
        : txn_kv_(std::move(txn_kv)),
          instance_id_(instance.instance_id()),
          instance_info_(instance) {}

InstanceRecycler::~InstanceRecycler() = default;

int InstanceRecycler::init() {
    for (auto& obj_info : instance_info_.obj_info()) {
        S3Conf s3_conf;
        s3_conf.ak = obj_info.ak();
        s3_conf.sk = obj_info.sk();
        if (obj_info.has_encryption_info()) {
            AkSkPair plain_ak_sk_pair;
            int ret = decrypt_ak_sk_helper(obj_info.ak(), obj_info.sk(), obj_info.encryption_info(),
                                           &plain_ak_sk_pair);
            if (ret != 0) {
                LOG(WARNING) << "fail to decrypt ak sk. instance_id: " << instance_id_
                             << " obj_info: " << proto_to_json(obj_info);
            } else {
                s3_conf.ak = std::move(plain_ak_sk_pair.first);
                s3_conf.sk = std::move(plain_ak_sk_pair.second);
            }
        }
        s3_conf.endpoint = obj_info.endpoint();
        s3_conf.region = obj_info.region();
        s3_conf.bucket = obj_info.bucket();
        s3_conf.prefix = obj_info.prefix();
#ifdef UNIT_TEST
        auto accessor = std::make_shared<MockAccessor>(s3_conf);
#else
        auto accessor = std::make_shared<S3Accessor>(std::move(s3_conf));
#endif
        if (accessor->init() != 0) [[unlikely]] {
            LOG(WARNING) << "failed to init object accessor, instance_id=" << instance_id_;
            return -1;
        }
        accessor_map_.emplace(obj_info.id(), std::move(accessor));
    }
    return 0;
}

void InstanceRecycler::recycle_instance() {
    LOG_INFO("begin to remove all data in instance").tag("instance_id", instance_id_);

    bool success = true;
    using namespace std::chrono;
    auto start_time = steady_clock::now();

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG(INFO) << (success ? "successfully" : "failed to")
                  << " remove all data in instance, cost=" << cost
                  << "s, instance_id=" << instance_id_;
    });

    int ret = 0;
    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        LOG(WARNING) << "failed to create txn";
        success = false;
        return;
    }
    LOG(INFO) << "begin to delete all kv, instance_id=" << instance_id_;
    // delete kv before deleting objects to prevent the checker from misjudging data loss
    std::string start_txn_key = txn_key_prefix(instance_id_);
    std::string end_txn_key = txn_key_prefix(instance_id_ + '\x00');
    txn->remove(start_txn_key, end_txn_key);
    // 0:instance_id  1:db_id  2:tbl_id  3:partition_id
    std::string start_version_key = version_key({instance_id_, 0, 0, 0});
    std::string end_version_key = version_key({instance_id_, INT64_MAX, 0, 0});
    txn->remove(start_txn_key, end_txn_key);
    std::string start_meta_key = meta_key_prefix(instance_id_);
    std::string end_meta_key = meta_key_prefix(instance_id_ + '\x00');
    txn->remove(start_meta_key, end_meta_key);
    std::string start_recycle_key = recycle_key_prefix(instance_id_);
    std::string end_recycle_key = recycle_key_prefix(instance_id_ + '\x00');
    txn->remove(start_recycle_key, end_recycle_key);
    std::string start_stats_tablet_key = stats_tablet_key({instance_id_, 0, 0, 0, 0});
    std::string end_stats_tablet_key = stats_tablet_key({instance_id_, INT64_MAX, 0, 0, 0});
    txn->remove(start_stats_tablet_key, end_stats_tablet_key);
    std::string start_copy_key = copy_key_prefix(instance_id_);
    std::string end_copy_key = copy_key_prefix(instance_id_ + '\x00');
    txn->remove(start_copy_key, end_copy_key);
    // should not remove job key range, because we need to reserve job recycle kv
    // 0:instance_id  1:table_id  2:index_id  3:part_id  4:tablet_id
    std::string start_job_tablet_key = job_tablet_key({instance_id_, 0, 0, 0, 0});
    std::string end_job_tablet_key = job_tablet_key({instance_id_, INT64_MAX, 0, 0, 0});
    txn->remove(start_job_tablet_key, end_job_tablet_key);
    ret = txn->commit();
    if (ret != 0) {
        LOG(WARNING) << "failed to delete all kv, instance_id=" << instance_id_;
        success = false;
    }

    for (auto& [_, accessor] : accessor_map_) {
        LOG(INFO) << "begin to delete all objects in " << accessor->path();
        ret = accessor->delete_objects_by_prefix("");
        if (ret == 0) {
            LOG(INFO) << "successfully delete all objects in " << accessor->path();
        } else { // no need to log, because S3Accessor has logged this error
            success = false;
        }
    }

    if (success) {
        // remove instance kv
        ret = txn_kv_->create_txn(&txn);
        if (ret != 0) {
            LOG(WARNING) << "failed to create txn";
            return;
        }
        std::string key;
        instance_key({instance_id_}, &key);
        txn->remove(key);
        ret = txn->commit();
        if (ret != 0) {
            LOG(WARNING) << "failed to delete instance kv, instance_id=" << instance_id_;
        }
    }
}

void InstanceRecycler::recycle_indexes() {
    int num_scanned = 0;
    int num_expired = 0;
    int num_recycled = 0;

    RecycleIndexKeyInfo index_key_info0 {instance_id_, 0};
    RecycleIndexKeyInfo index_key_info1 {instance_id_, INT64_MAX};
    std::string index_key0;
    std::string index_key1;
    recycle_index_key(index_key_info0, &index_key0);
    recycle_index_key(index_key_info1, &index_key1);

    LOG_INFO("begin to recycle indexes").tag("instance_id", instance_id_);

    using namespace std::chrono;
    auto start_time = steady_clock::now();

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG_INFO("recycle indexes finished, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_expired", num_expired)
                .tag("num_recycled", num_recycled);
    });

    // Elements in `index_keys` has the same lifetime as `it` in `scan_and_recycle`
    std::vector<std::string_view> index_keys;
    auto recycle_func = [&num_scanned, &num_expired, &num_recycled, &index_keys, this](
                                std::string_view k, std::string_view v) -> int {
        ++num_scanned;
        RecycleIndexPB index_pb;
        if (!index_pb.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed recycle index value").tag("key", hex(k));
            return -1;
        }
        int64_t current_time = ::time(nullptr);
        int64_t expiration =
                index_pb.expiration() > 0 ? index_pb.expiration() : index_pb.creation_time();
        if (current_time < expiration + config::retention_seconds) {
            // not expired
            return 0;
        }
        ++num_expired;
        // decode index_id
        auto k1 = k;
        k1.remove_prefix(1);
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        decode_key(&k1, &out);
        // 0x01 "recycle" ${instance_id} "index" ${index_id} -> RecycleIndexPB
        auto index_id = std::get<int64_t>(std::get<0>(out[3]));
        LOG(INFO) << "begin to recycle index, instance_id=" << instance_id_
                  << " table_id=" << index_pb.table_id() << " index_id=" << index_id
                  << " type=" << RecycleIndexPB_Type_Name(index_pb.type());
        if (recycle_tablets(index_pb.table_id(), index_id) != 0) {
            LOG_WARNING("failed to recycle tablets under index")
                    .tag("table_id", index_pb.table_id())
                    .tag("instance_id", instance_id_)
                    .tag("index_id", index_id);
            return -1;
        }
        ++num_recycled;
        index_keys.push_back(k);
        return 0;
    };

    auto loop_done = [&index_keys, this]() -> int {
        if (index_keys.empty()) return 0;
        std::unique_ptr<int, std::function<void(int*)>> defer((int*)0x01,
                                                              [&](int*) { index_keys.clear(); });
        if (0 != txn_remove(txn_kv_.get(), index_keys)) {
            LOG(WARNING) << "failed to delete recycle index kv, instance_id=" << instance_id_;
            return -1;
        }
        return 0;
    };

    scan_and_recycle(index_key0, index_key1, std::move(recycle_func), std::move(loop_done));
}

void InstanceRecycler::recycle_partitions() {
    int num_scanned = 0;
    int num_expired = 0;
    int num_recycled = 0;

    RecyclePartKeyInfo part_key_info0 {instance_id_, 0};
    RecyclePartKeyInfo part_key_info1 {instance_id_, INT64_MAX};
    std::string part_key0;
    std::string part_key1;
    recycle_partition_key(part_key_info0, &part_key0);
    recycle_partition_key(part_key_info1, &part_key1);

    LOG_INFO("begin to recycle partitions").tag("instance_id", instance_id_);

    using namespace std::chrono;
    auto start_time = steady_clock::now();

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG_INFO("recycle partitions finished, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_expired", num_expired)
                .tag("num_recycled", num_recycled);
    });

    // Elements in `partition_keys` has the same lifetime as `it` in `scan_and_recycle`
    std::vector<std::string_view> partition_keys;
    auto recycle_func = [&num_scanned, &num_expired, &num_recycled, &partition_keys, this](
                                std::string_view k, std::string_view v) -> int {
        ++num_scanned;
        RecyclePartitionPB part_pb;
        if (!part_pb.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed recycle partition value").tag("key", hex(k));
            return -1;
        }
        int64_t current_time = ::time(nullptr);
        int64_t expiration =
                part_pb.expiration() > 0 ? part_pb.expiration() : part_pb.creation_time();
        if (current_time < expiration + config::retention_seconds) {
            // not expired
            return 0;
        }
        ++num_expired;
        // decode partition_id
        auto k1 = k;
        k1.remove_prefix(1);
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        decode_key(&k1, &out);
        // 0x01 "recycle" ${instance_id} "partition" ${partition_id} -> RecyclePartitionPB
        auto partition_id = std::get<int64_t>(std::get<0>(out[3]));
        LOG(INFO) << "begin to recycle partition, instance_id=" << instance_id_
                  << " table_id=" << part_pb.table_id() << " partition_id=" << partition_id
                  << " type=" << RecyclePartitionPB_Type_Name(part_pb.type());
        int ret = 0;
        for (int64_t index_id : part_pb.index_id()) {
            if (recycle_tablets(part_pb.table_id(), index_id, partition_id) != 0) {
                LOG_WARNING("failed to recycle tablets under partition")
                        .tag("table_id", part_pb.table_id())
                        .tag("instance_id", instance_id_)
                        .tag("index_id", index_id)
                        .tag("partition_id", partition_id);
                ret = -1;
            }
        }
        if (ret == 0) {
            ++num_recycled;
            partition_keys.push_back(k);
        }
        return ret;
    };

    auto loop_done = [&partition_keys, this]() -> int {
        if (partition_keys.empty()) return 0;
        std::unique_ptr<int, std::function<void(int*)>> defer(
                (int*)0x01, [&](int*) { partition_keys.clear(); });
        if (0 != txn_remove(txn_kv_.get(), partition_keys)) {
            LOG(WARNING) << "failed to delete recycle partition kv, instance_id=" << instance_id_;
            return -1;
        }
        return 0;
    };

    scan_and_recycle(part_key0, part_key1, std::move(recycle_func), std::move(loop_done));
}

int InstanceRecycler::recycle_tablets(int64_t table_id, int64_t index_id, int64_t partition_id) {
    int num_scanned = 0;
    int num_recycled = 0;

    std::string tablet_key_begin, tablet_key_end;
    std::string stats_key_begin, stats_key_end;
    std::string job_key_begin, job_key_end;

    if (partition_id > 0) {
        // recycle tablets in a partition belonging to the index
        meta_tablet_key({instance_id_, table_id, index_id, partition_id, 0}, &tablet_key_begin);
        meta_tablet_key({instance_id_, table_id, index_id, partition_id + 1, 0}, &tablet_key_end);
        stats_tablet_key({instance_id_, table_id, index_id, partition_id, 0}, &stats_key_begin);
        stats_tablet_key({instance_id_, table_id, index_id, partition_id + 1, 0}, &stats_key_end);
        job_tablet_key({instance_id_, table_id, index_id, partition_id, 0}, &job_key_begin);
        job_tablet_key({instance_id_, table_id, index_id, partition_id + 1, 0}, &job_key_end);
    } else {
        // recycle tablets in the index
        meta_tablet_key({instance_id_, table_id, index_id, 0, 0}, &tablet_key_begin);
        meta_tablet_key({instance_id_, table_id, index_id + 1, 0, 0}, &tablet_key_end);
        stats_tablet_key({instance_id_, table_id, index_id, 0, 0}, &stats_key_begin);
        stats_tablet_key({instance_id_, table_id, index_id + 1, 0, 0}, &stats_key_end);
        job_tablet_key({instance_id_, table_id, index_id, 0, 0}, &job_key_begin);
        job_tablet_key({instance_id_, table_id, index_id + 1, 0, 0}, &job_key_end);
    }

    LOG_INFO("begin to recycle tablets")
            .tag("table_id", table_id)
            .tag("index_id", index_id)
            .tag("partition_id", partition_id);

    using namespace std::chrono;
    auto start_time = steady_clock::now();

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG_INFO("recycle tablets finished, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("table_id", table_id)
                .tag("index_id", index_id)
                .tag("num_scanned", num_scanned)
                .tag("num_recycled", num_recycled);
    });

    // Elements in `tablet_keys` has the same lifetime as `it` in `scan_and_recycle`
    std::vector<std::string_view> tablet_keys;
    std::vector<std::string> tablet_idx_keys;
    bool use_range_remove = true;
    auto recycle_func = [&num_scanned, &num_recycled, &tablet_keys, &tablet_idx_keys,
                         &use_range_remove, this](std::string_view k, std::string_view v) -> int {
        ++num_scanned;
        doris::TabletMetaPB tablet_meta_pb;
        if (!tablet_meta_pb.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed tablet meta").tag("key", hex(k));
            use_range_remove = false;
            return -1;
        }
        tablet_idx_keys.push_back(meta_tablet_idx_key({instance_id_, tablet_meta_pb.tablet_id()}));
        if (recycle_tablet(tablet_meta_pb.tablet_id()) != 0) {
            LOG_WARNING("failed to recycle tablet")
                    .tag("instance_id", instance_id_)
                    .tag("tablet_id", tablet_meta_pb.tablet_id());
            use_range_remove = false;
            return -1;
        }
        ++num_recycled;
        tablet_keys.push_back(k);
        return 0;
    };

    auto loop_done = [&tablet_keys, &tablet_idx_keys, &use_range_remove, this]() -> int {
        if (tablet_keys.empty() && tablet_idx_keys.empty()) return 0;
        std::unique_ptr<int, std::function<void(int*)>> defer((int*)0x01, [&](int*) {
            tablet_keys.clear();
            tablet_idx_keys.clear();
            use_range_remove = true;
        });
        std::unique_ptr<Transaction> txn;
        if (txn_kv_->create_txn(&txn) != 0) {
            LOG(WARNING) << "failed to delete tablet meta kv, instance_id=" << instance_id_;
            return -1;
        }
        std::string tablet_key_end;
        if (!tablet_keys.empty()) {
            if (use_range_remove) {
                tablet_key_end = std::string(tablet_keys.back()) + '\x00';
                txn->remove(tablet_keys.front(), tablet_key_end);
            } else {
                for (auto k : tablet_idx_keys) {
                    txn->remove(k);
                }
            }
        }
        for (auto& k : tablet_idx_keys) {
            txn->remove(k);
        }
        if (txn->commit() != 0) {
            LOG(WARNING) << "failed to delete tablet meta kv, instance_id=" << instance_id_;
            return -1;
        }
        return 0;
    };

    int ret = scan_and_recycle(tablet_key_begin, tablet_key_end, std::move(recycle_func),
                               std::move(loop_done));

    // directly remove tablet stats and tablet jobs of these dropped index or partition
    std::unique_ptr<Transaction> txn;
    if (txn_kv_->create_txn(&txn) != 0) {
        LOG(WARNING) << "failed to delete tablet job or stats key, instance_id=" << instance_id_;
        return -1;
    }
    txn->remove(stats_key_begin, stats_key_end);
    txn->remove(job_key_begin, job_key_end);
    if (txn->commit() != 0) {
        LOG(WARNING) << "failed to delete tablet job or stats key, instance_id=" << instance_id_;
        return -1;
    }

    return ret;
}

int InstanceRecycler::delete_rowset_data(const doris::RowsetMetaPB& rs_meta_pb) {
    int64_t num_segments = rs_meta_pb.num_segments();
    if (num_segments <= 0) return 0;
    auto it = accessor_map_.find(rs_meta_pb.resource_id());
    if (it == accessor_map_.end()) {
        LOG_WARNING("instance has no such resource id")
                .tag("instance_id", instance_id_)
                .tag("resource_id", rs_meta_pb.resource_id());
        return -1;
    }
    auto& accessor = it->second;
    const auto& rowset_id = rs_meta_pb.rowset_id_v2();
    int64_t tablet_id = rs_meta_pb.tablet_id();
    // process inverted indexes
    std::vector<int64_t> index_ids;
    index_ids.reserve(rs_meta_pb.tablet_schema().index().size());
    for (auto& index_pb : rs_meta_pb.tablet_schema().index()) {
        index_ids.push_back(index_pb.index_id());
    }

    std::vector<std::string> file_paths;
    file_paths.reserve(num_segments * (1 + index_ids.size()));
    for (int64_t i = 0; i < num_segments; ++i) {
        file_paths.push_back(segment_path(tablet_id, rowset_id, i));
        for (int64_t index_id : index_ids) {
            file_paths.push_back(inverted_index_path(tablet_id, rowset_id, i, index_id));
        }
    }
    return accessor->delete_objects(file_paths);
}

int InstanceRecycler::delete_rowset_data(const std::vector<doris::RowsetMetaPB>& rowsets) {
    // resource_id -> file_paths
    std::map<std::string, std::vector<std::string>> resource_file_paths;
    for (auto& rs : rowsets) {
        auto it = accessor_map_.find(rs.resource_id());
        if (it == accessor_map_.end()) [[unlikely]] { // impossible
            LOG_WARNING("instance has no such resource id")
                    .tag("instance_id", instance_id_)
                    .tag("resource_id", rs.resource_id());
            continue;
        }
        auto& file_paths = resource_file_paths[rs.resource_id()];
        const auto& rowset_id = rs.rowset_id_v2();
        int64_t tablet_id = rs.tablet_id();
        int64_t num_segments = rs.num_segments();
        if (num_segments <= 0) continue;
        // process inverted indexes
        std::vector<int64_t> index_ids;
        index_ids.reserve(rs.tablet_schema().index().size());
        for (auto& index_pb : rs.tablet_schema().index()) {
            index_ids.push_back(index_pb.index_id());
        }
        for (int64_t i = 0; i < num_segments; ++i) {
            file_paths.push_back(segment_path(tablet_id, rowset_id, i));
            for (int64_t index_id : index_ids) {
                file_paths.push_back(inverted_index_path(tablet_id, rowset_id, i, index_id));
            }
        }
    }
    int ret = 0;
    for (auto& [resource_id, file_paths] : resource_file_paths) {
        auto& accessor = accessor_map_[resource_id];
        DCHECK(accessor);
        if (accessor->delete_objects(file_paths) != 0) {
            ret = -1;
        }
    }
    return ret;
}

int InstanceRecycler::delete_rowset_data(const std::string& resource_id, int64_t tablet_id,
                                         const std::string& rowset_id) {
    auto it = accessor_map_.find(resource_id);
    if (it == accessor_map_.end()) {
        LOG_WARNING("instance has no such resource id")
                .tag("instance_id", instance_id_)
                .tag("resource_id", resource_id);
        return -1;
    }
    auto& accessor = it->second;
    return accessor->delete_objects_by_prefix(rowset_path_prefix(tablet_id, rowset_id));
}

int InstanceRecycler::recycle_tablet(int64_t tablet_id) {
    LOG_INFO("begin to recycle rowsets in a dropped tablet")
            .tag("instance_id", instance_id_)
            .tag("tablet_id", tablet_id);

    using namespace std::chrono;
    auto start_time = steady_clock::now();

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG_INFO("recycle rowsets finished, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("tablet_id", tablet_id);
    });

    // delete all rowset kv in this tablet
    std::string rs_key0 = meta_rowset_key({instance_id_, tablet_id, 0});
    std::string rs_key1 = meta_rowset_key({instance_id_, tablet_id + 1, 0});
    std::string recyc_rs_key0 = recycle_rowset_key({instance_id_, tablet_id, ""});
    std::string recyc_rs_key1 = recycle_rowset_key({instance_id_, tablet_id + 1, ""});

    int ret = 0;
    std::unique_ptr<Transaction> txn;
    if (txn_kv_->create_txn(&txn) != 0) {
        LOG(WARNING) << "failed to delete rowset kv of tablet " << tablet_id;
        ret = -1;
    }
    txn->remove(rs_key0, rs_key1);
    txn->remove(recyc_rs_key0, recyc_rs_key1);
    if (txn->commit() != 0) {
        LOG(WARNING) << "failed to delete rowset kv of tablet " << tablet_id;
        ret = -1;
    }

    // delete all rowset data in this tablet
    for (auto& [_, accessor] : accessor_map_) {
        if (accessor->delete_objects_by_prefix(tablet_path_prefix(tablet_id)) != 0) {
            LOG(WARNING) << "failed to delete rowset data of tablet " << tablet_id
                         << " s3_path=" << accessor->path();
            ret = -1;
        }
    }
    return ret;
}

void InstanceRecycler::recycle_rowsets() {
    int num_scanned = 0;
    int num_expired = 0;
    std::atomic_int num_recycled = 0;

    RecycleRowsetKeyInfo recyc_rs_key_info0 {instance_id_, 0, ""};
    RecycleRowsetKeyInfo recyc_rs_key_info1 {instance_id_, INT64_MAX, ""};
    std::string recyc_rs_key0;
    std::string recyc_rs_key1;
    recycle_rowset_key(recyc_rs_key_info0, &recyc_rs_key0);
    recycle_rowset_key(recyc_rs_key_info1, &recyc_rs_key1);

    LOG_INFO("begin to recycle rowsets").tag("instance_id", instance_id_);

    using namespace std::chrono;
    auto start_time = steady_clock::now();

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG_INFO("recycle rowsets finished, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_expired", num_expired)
                .tag("num_recycled", num_recycled);
    });

    std::vector<std::string> rowset_keys;
    std::vector<doris::RowsetMetaPB> rowsets;

    // Store keys of rowset recycled by background workers
    std::mutex async_recycled_rowset_keys_mutex;
    std::vector<std::string> async_recycled_rowset_keys;
    auto worker_pool =
            std::make_unique<SimpleThreadPool>(config::instance_recycler_worker_pool_size);
    worker_pool->start();
    auto delete_rowset_data_by_prefix = [&](std::string key, const std::string& resource_id,
                                            int64_t tablet_id, const std::string& rowset_id) {
        // Try to delete rowset data in background thread
        int ret = worker_pool->submit_with_timeout(
                [&, resource_id, tablet_id, rowset_id, key]() mutable {
                    if (delete_rowset_data(resource_id, tablet_id, rowset_id) != 0) {
                        LOG(WARNING) << "failed to delete rowset data, key=" << hex(key);
                        return;
                    }
                    std::vector<std::string> keys;
                    {
                        std::lock_guard lock(async_recycled_rowset_keys_mutex);
                        async_recycled_rowset_keys.push_back(std::move(key));
                        if (async_recycled_rowset_keys.size() > 100) {
                            keys.swap(async_recycled_rowset_keys);
                        }
                    }
                    if (keys.empty()) return;
                    if (txn_remove(txn_kv_.get(), keys) != 0) {
                        LOG(WARNING) << "failed to delete recycle rowset kv, instance_id="
                                     << instance_id_;
                    } else {
                        num_recycled.fetch_add(keys.size(), std::memory_order_relaxed);
                    }
                },
                0);
        if (ret == 0) return 0;
        // Submit task failed, delete rowset data in current thread
        if (delete_rowset_data(resource_id, tablet_id, rowset_id) != 0) {
            LOG(WARNING) << "failed to delete rowset data, key=" << hex(key);
            return -1;
        }
        rowset_keys.push_back(std::move(key));
        return 0;
    };

    auto handle_rowset_kv = [&](std::string_view k, std::string_view v) -> int {
        ++num_scanned;
        RecycleRowsetPB rowset;
        if (!rowset.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed recycle rowset").tag("key", hex(k));
            return -1;
        }
        int64_t current_time = ::time(nullptr);
        int64_t expiration = rowset.expiration() > 0 ? rowset.expiration() : rowset.creation_time();
        if (current_time < expiration + config::retention_seconds) {
            // not expired
            return 0;
        }
        ++num_expired;
        if (!rowset.has_type()) {                         // old version `RecycleRowsetPB`
            if (!rowset.has_resource_id()) [[unlikely]] { // impossible
                LOG_WARNING("rowset meta has empty resource id").tag("key", k);
                return -1;
            }
            // decode rowset_id
            auto k1 = k;
            k1.remove_prefix(1);
            std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
            decode_key(&k1, &out);
            // 0x01 "recycle" ${instance_id} "rowset" ${tablet_id} ${rowset_id} -> RecycleRowsetPB
            const auto& rowset_id = std::get<std::string>(std::get<0>(out[4]));
            LOG(INFO) << "delete rowset data, instance_id=" << instance_id_
                      << " tablet_id=" << rowset.tablet_id() << " rowset_id=" << rowset_id;
            if (delete_rowset_data_by_prefix(std::string(k), rowset.resource_id(),
                                             rowset.tablet_id(), rowset_id) != 0) {
                return -1;
            }
            return 0;
        }
        // TODO(plat1ko): check rowset not referenced
        auto rowset_meta = rowset.mutable_rowset_meta();
        if (!rowset_meta->has_resource_id()) [[unlikely]] { // impossible
            LOG_WARNING("rowset meta has empty resource id").tag("key", k);
            return -1;
        }
        LOG(INFO) << "delete rowset data, instance_id=" << instance_id_
                  << " tablet_id=" << rowset_meta->tablet_id()
                  << " rowset_id=" << rowset_meta->rowset_id_v2() << " version=["
                  << rowset_meta->start_version() << '-' << rowset_meta->end_version()
                  << "] txn_id=" << rowset_meta->txn_id()
                  << " type=" << RecycleRowsetPB_Type_Name(rowset.type());
        if (rowset.type() == RecycleRowsetPB::PREPARE) {
            // unable to calculate file path, can only be deleted by rowset id prefix
            if (delete_rowset_data_by_prefix(std::string(k), rowset_meta->resource_id(),
                                             rowset_meta->tablet_id(),
                                             rowset_meta->rowset_id_v2()) != 0) {
                return -1;
            }
        } else {
            rowset_keys.push_back(std::string(k));
            if (rowset_meta->num_segments() > 0) { // Skip empty rowset
                rowsets.push_back(std::move(*rowset_meta));
            }
        }
        return 0;
    };

    auto loop_done = [&]() -> int {
        std::vector<std::string> rowset_keys_to_delete;
        std::vector<doris::RowsetMetaPB> rowsets_to_delete;
        rowset_keys_to_delete.swap(rowset_keys);
        rowsets_to_delete.swap(rowsets);
        worker_pool->submit([&, rowset_keys_to_delete = std::move(rowset_keys_to_delete),
                             rowsets_to_delete = std::move(rowsets_to_delete)]() {
            if (delete_rowset_data(rowsets_to_delete) != 0) {
                LOG(WARNING) << "failed to delete rowset data, instance_id=" << instance_id_;
                return;
            }
            if (txn_remove(txn_kv_.get(), rowset_keys_to_delete) != 0) {
                LOG(WARNING) << "failed to delete recycle rowset kv, instance_id=" << instance_id_;
                return;
            }
            num_recycled.fetch_add(rowset_keys.size(), std::memory_order_relaxed);
        });
        return 0;
    };

    scan_and_recycle(recyc_rs_key0, recyc_rs_key1, std::move(handle_rowset_kv),
                     std::move(loop_done));
    worker_pool->stop();

    if (!async_recycled_rowset_keys.empty()) {
        if (txn_remove(txn_kv_.get(), async_recycled_rowset_keys) != 0) {
            LOG(WARNING) << "failed to delete recycle rowset kv, instance_id=" << instance_id_;
        } else {
            num_recycled.fetch_add(async_recycled_rowset_keys.size(), std::memory_order_relaxed);
        }
    }
}

void InstanceRecycler::recycle_tmp_rowsets() {
    int num_scanned = 0;
    int num_expired = 0;
    int num_recycled = 0;

    MetaRowsetTmpKeyInfo tmp_rs_key_info0 {instance_id_, 0, 0};
    MetaRowsetTmpKeyInfo tmp_rs_key_info1 {instance_id_, INT64_MAX, 0};
    std::string tmp_rs_key0;
    std::string tmp_rs_key1;
    meta_rowset_tmp_key(tmp_rs_key_info0, &tmp_rs_key0);
    meta_rowset_tmp_key(tmp_rs_key_info1, &tmp_rs_key1);

    LOG_INFO("begin to recycle tmp rowsets").tag("instance_id", instance_id_);

    using namespace std::chrono;
    auto start_time = steady_clock::now();

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG_INFO("recycle tmp rowsets finished, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_expired", num_expired)
                .tag("num_recycled", num_recycled);
    });

    // Elements in `tmp_rowset_keys` has the same lifetime as `it`
    std::vector<std::string_view> tmp_rowset_keys;
    std::vector<doris::RowsetMetaPB> tmp_rowsets;

    auto handle_rowset_kv = [&num_scanned, &num_expired, &tmp_rowset_keys, &tmp_rowsets, this](
                                    std::string_view k, std::string_view v) -> int {
        ++num_scanned;
        doris::RowsetMetaPB rowset;
        if (!rowset.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed rowset meta").tag("key", hex(k));
            return -1;
        }
        int64_t current_time = ::time(nullptr);
        int64_t expiration =
                rowset.txn_expiration() > 0 ? rowset.txn_expiration() : rowset.creation_time();
        if (current_time < expiration + config::retention_seconds) {
            // not expired
            return 0;
        }
        ++num_expired;
        if (!rowset.has_resource_id()) {
            if (rowset.num_segments() > 0) [[unlikely]] { // impossible
                LOG_WARNING("rowset meta has empty resource id").tag("key", k);
                return -1;
            }
            // might be a delete pred rowset
            tmp_rowset_keys.push_back(k);
            return 0;
        }
        // TODO(plat1ko): check rowset not referenced
        LOG(INFO) << "delete rowset data, instance_id=" << instance_id_
                  << " tablet_id=" << rowset.tablet_id() << " rowset_id=" << rowset.rowset_id_v2()
                  << " version=[" << rowset.start_version() << '-' << rowset.end_version()
                  << "] txn_id=" << rowset.txn_id();
        tmp_rowset_keys.push_back(k);
        if (rowset.num_segments() > 0) { // Skip empty rowset
            tmp_rowsets.push_back(std::move(rowset));
        }
        return 0;
    };

    auto loop_done = [&tmp_rowset_keys, &tmp_rowsets, &num_recycled, this]() -> int {
        std::unique_ptr<int, std::function<void(int*)>> defer((int*)0x01, [&](int*) {
            tmp_rowset_keys.clear();
            tmp_rowsets.clear();
        });
        if (delete_rowset_data(tmp_rowsets) != 0) {
            LOG(WARNING) << "failed to delete tmp rowset data, instance_id=" << instance_id_;
            return -1;
        }
        if (txn_remove(txn_kv_.get(), tmp_rowset_keys) != 0) {
            LOG(WARNING) << "failed to delete tmp rowset kv, instance_id=" << instance_id_;
            return -1;
        }
        num_recycled += tmp_rowset_keys.size();
        return 0;
    };

    scan_and_recycle(tmp_rs_key0, tmp_rs_key1, std::move(handle_rowset_kv), std::move(loop_done));
}

int InstanceRecycler::scan_and_recycle(
        std::string begin, std::string_view end,
        std::function<int(std::string_view k, std::string_view v)> recycle_func,
        std::function<int()> loop_done) {
    int ret = 0;
    std::unique_ptr<RangeGetIterator> it;
    do {
        ret = txn_get(txn_kv_.get(), begin, end, it);
        if (ret != 0) {
            LOG(WARNING) << "failed to get kv, key=" << begin << " ret=" << ret;
            return -1;
        }
        VLOG_DEBUG << "fetch " << it->size() << " kv";
        if (!it->has_next()) {
            VLOG_DEBUG << "no keys in the given range, begin=" << hex(begin) << " end=" << hex(end);
            break;
        }
        while (it->has_next()) {
            // recycle corresponding resources
            auto [k, v] = it->next();
            if (!it->has_next()) {
                begin = k;
                VLOG_DEBUG << "iterator has no more kvs. key=" << hex(k);
            }
            if (recycle_func(k, v) != 0) ret = -1;
        }
        begin.push_back('\x00'); // Update to next smallest key for iteration
        if (loop_done) {
            if (loop_done() != 0) ret = -1;
        }
    } while (it->more() && is_working());
    return ret;
}

void InstanceRecycler::abort_timeout_txn() {
    int num_scanned = 0;
    int num_timeout = 0;
    int num_abort = 0;

    TxnRunningKeyInfo txn_running_key_info0 {instance_id_, 0, 0};
    TxnRunningKeyInfo txn_running_key_info1 {instance_id_, INT64_MAX, INT64_MAX};
    std::string begin_txn_running_key;
    std::string end_txn_running_key;
    txn_running_key(txn_running_key_info0, &begin_txn_running_key);
    txn_running_key(txn_running_key_info1, &end_txn_running_key);

    LOG_INFO("begin to abort timeout txn").tag("instance_id", instance_id_);

    using namespace std::chrono;
    auto start_time = steady_clock::now();

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG_INFO("end to abort timeout txn, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_timeout", num_timeout)
                .tag("num_abort", num_abort);
    });

    int64_t current_time =
            duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

    auto handle_txn_running_kv = [&num_scanned, &num_timeout, &num_abort, &current_time, this](
                                         std::string_view k, std::string_view v) -> int {
        ++num_scanned;
        TxnRunningPB txn_running_pb;
        if (!txn_running_pb.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed txn_running_pb").tag("key", hex(k));
            return -1;
        }
        if (txn_running_pb.timeout_time() > current_time) {
            return 0;
        }
        ++num_timeout;

        std::unique_ptr<Transaction> txn;
        int ret = txn_kv_->create_txn(&txn);
        if (ret != 0) {
            LOG_ERROR("failed to create txn ret={}", ret).tag("key", hex(k));
            return -1;
        }
        std::string_view k1 = k;
        //TxnRunningKeyInfo 0:instance_id  1:db_id  2:txn_id
        k1.remove_prefix(1); // Remove key space
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        if (decode_key(&k1, &out) != 0) {
            LOG_ERROR("failed to decode key").tag("key", hex(k));
            return -1;
        }
        int64_t db_id = std::get<int64_t>(std::get<0>(out[3]));
        int64_t txn_id = std::get<int64_t>(std::get<0>(out[4]));
        VLOG_DEBUG << "instance_id=" << instance_id_ << " db_id=" << db_id << " txn_id=" << txn_id;
        // Update txn_info
        std::string txn_inf_key, txn_inf_val;
        txn_info_key({instance_id_, db_id, txn_id}, &txn_inf_key);
        ret = txn->get(txn_inf_key, &txn_inf_val);
        if (ret != 0) {
            LOG_WARNING("failed to get txn info ret={}", ret).tag("key", hex(txn_inf_key));
            return -1;
        }
        TxnInfoPB txn_info;
        if (!txn_info.ParseFromString(txn_inf_val)) {
            LOG_WARNING("failed to parse txn info").tag("key", hex(k));
            return -1;
        }
        txn_info.set_status(TxnStatusPB::TXN_STATUS_ABORTED);
        txn_info.set_finish_time(current_time);
        txn_info.set_reason("timeout");
        VLOG_DEBUG << "txn_info=" << txn_info.DebugString();
        txn_inf_val.clear();
        if (!txn_info.SerializeToString(&txn_inf_val)) {
            LOG_WARNING("failed to serialize txn info").tag("key", hex(k));
            return -1;
        }
        txn->put(txn_inf_key, txn_inf_val);
        VLOG_DEBUG << "txn->put, txn_inf_key=" << hex(txn_inf_key);
        // Put recycle txn key
        std::string recyc_txn_key, recyc_txn_val;
        recycle_txn_key({instance_id_, db_id, txn_id}, &recyc_txn_key);
        RecycleTxnPB recycle_txn_pb;
        recycle_txn_pb.set_creation_time(current_time);
        recycle_txn_pb.set_label(txn_info.label());
        if (!recycle_txn_pb.SerializeToString(&recyc_txn_val)) {
            LOG_WARNING("failed to serialize txn recycle info")
                    .tag("key", hex(k))
                    .tag("db_id", db_id)
                    .tag("txn_id", txn_id);
            return -1;
        }
        txn->put(recyc_txn_key, recyc_txn_val);
        // Remove txn running key
        txn->remove(k);
        ret = txn->commit();
        if (ret != 0) {
            LOG_WARNING("failed to commit txn ret={}", ret)
                    .tag("key", hex(k))
                    .tag("db_id", db_id)
                    .tag("txn_id", txn_id);
            return -1;
        }
        ++num_abort;
        return 0;
    };

    scan_and_recycle(begin_txn_running_key, end_txn_running_key, std::move(handle_txn_running_kv));
}

void InstanceRecycler::recycle_expired_txn_label() {
    int num_scanned = 0;
    int num_expired = 0;
    int num_recycled = 0;

    RecycleTxnKeyInfo recycle_txn_key_info0 {instance_id_, 0, 0};
    RecycleTxnKeyInfo recycle_txn_key_info1 {instance_id_, INT64_MAX, INT64_MAX};
    std::string begin_recycle_txn_key;
    std::string end_recycle_txn_key;
    recycle_txn_key(recycle_txn_key_info0, &begin_recycle_txn_key);
    recycle_txn_key(recycle_txn_key_info1, &end_recycle_txn_key);

    LOG_INFO("begin to recycle expire txn").tag("instance_id", instance_id_);

    using namespace std::chrono;
    auto start_time = steady_clock::now();

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG_INFO("end to recycle expired txn, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_expired", num_expired)
                .tag("num_recycled", num_recycled);
    });

    int64_t current_time =
            duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

    auto handle_recycle_txn_kv = [&num_scanned, &num_expired, &num_recycled, &current_time, this](
                                         std::string_view k, std::string_view v) -> int {
        ++num_scanned;
        RecycleTxnPB recycle_txn_pb;
        if (!recycle_txn_pb.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed txn_running_pb").tag("key", hex(k));
            return -1;
        }
        if ((recycle_txn_pb.has_immediate() && recycle_txn_pb.immediate()) ||
            (recycle_txn_pb.creation_time() + config::label_keep_max_second <= current_time)) {
            LOG_INFO("found recycle txn").tag("key", hex(k));
            num_expired++;
        } else {
            return 0;
        }
        std::string_view k1 = k;
        //RecycleTxnKeyInfo 0:instance_id  1:db_id  2:txn_id
        k1.remove_prefix(1); // Remove key space
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        int ret = decode_key(&k1, &out);
        if (ret != 0) {
            LOG_ERROR("failed to decode key, ret={}", ret).tag("key", hex(k));
            return -1;
        }
        int64_t db_id = std::get<int64_t>(std::get<0>(out[3]));
        int64_t txn_id = std::get<int64_t>(std::get<0>(out[4]));
        VLOG_DEBUG << "instance_id=" << instance_id_ << " db_id=" << db_id << " txn_id=" << txn_id;
        std::unique_ptr<Transaction> txn;
        ret = txn_kv_->create_txn(&txn);
        if (ret != 0) {
            LOG_ERROR("failed to create txn ret={}", ret).tag("key", hex(k));
            return -1;
        }
        // Remove txn index kv
        auto index_key = txn_index_key({instance_id_, txn_id});
        txn->remove(index_key);
        // Remove txn info kv
        std::string info_key, info_val;
        txn_info_key({instance_id_, db_id, txn_id}, &info_key);
        ret = txn->get(info_key, &info_val);
        if (ret != 0) {
            LOG_WARNING("failed to get txn info ret={}", ret).tag("key", hex(info_key));
            return -1;
        }
        TxnInfoPB txn_info;
        if (!txn_info.ParseFromString(info_val)) {
            LOG_WARNING("failed to parse txn info").tag("key", hex(info_key));
            return -1;
        }
        txn->remove(info_key);
        // Update txn label
        std::string label_key, label_val;
        txn_label_key({instance_id_, db_id, txn_info.label()}, &label_key);
        ret = txn->get(label_key, &label_val);
        if (ret != 0) {
            LOG(WARNING) << "failed to get txn label, txn_id=" << txn_id << " key=" << label_key;
            return -1;
        }
        TxnLabelPB txn_label;
        if (!txn_label.ParseFromArray(label_val.data(), label_val.size() - VERSION_STAMP_LEN)) {
            LOG_WARNING("failed to parse txn label").tag("key", hex(label_key));
            return -1;
        }
        auto it = std::find(txn_label.txn_ids().begin(), txn_label.txn_ids().end(), txn_id);
        if (it != txn_label.txn_ids().end()) {
            txn_label.mutable_txn_ids()->erase(it);
        }
        if (txn_label.txn_ids().empty()) {
            txn->remove(label_key);
        } else {
            if (!txn_label.SerializeToString(&label_val)) {
                LOG(WARNING) << "failed to serialize txn label, key=" << hex(label_key);
                return -1;
            }
            txn->atomic_set_ver_value(label_key, label_val);
        }
        // Remove recycle txn kv
        txn->remove(k);
        ret = txn->commit();
        if (ret != 0) {
            LOG(WARNING) << "failed to delete expired txn, ret=" << ret << " key=" << hex(k);
            return -1;
        }
        ++num_recycled;
        LOG(INFO) << "recycle expired txn, key=" << hex(k);
        return 0;
    };

    scan_and_recycle(begin_recycle_txn_key, end_recycle_txn_key, std::move(handle_recycle_txn_kv));
}

void InstanceRecycler::recycle_copy_jobs() {
    int num_scanned = 0;
    int num_finished = 0;
    int num_expired = 0;
    int num_recycled = 0;

    LOG_INFO("begin to recycle copy jobs").tag("instance_id", instance_id_);

    using namespace std::chrono;
    auto start_time = steady_clock::now();

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG_INFO("recycle copy jobs finished, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_finished", num_finished)
                .tag("num_expired", num_expired)
                .tag("num_recycled", num_recycled);
    });

    CopyJobKeyInfo key_info0 {instance_id_, "", 0, "", 0};
    CopyJobKeyInfo key_info1 {instance_id_, "\xff", 0, "", 0};
    std::string key0;
    std::string key1;
    copy_job_key(key_info0, &key0);
    copy_job_key(key_info1, &key1);

    std::unordered_map<std::string, std::shared_ptr<ObjStoreAccessor>> stage_accessor_map;
    auto recycle_func = [&num_scanned, &num_finished, &num_expired, &num_recycled,
                         &stage_accessor_map, this](std::string_view k, std::string_view v) -> int {
        ++num_scanned;
        CopyJobPB copy_job;
        if (!copy_job.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed copy job").tag("key", hex(k));
            return -1;
        }

        // decode copy job key
        auto k1 = k;
        k1.remove_prefix(1);
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        decode_key(&k1, &out);
        // 0x01 "copy" ${instance_id} "job" ${stage_id} ${table_id} ${copy_id} ${group_id}
        // -> CopyJobPB
        const auto& stage_id = std::get<std::string>(std::get<0>(out[3]));
        const auto& table_id = std::get<int64_t>(std::get<0>(out[4]));
        const auto& copy_id = std::get<std::string>(std::get<0>(out[5]));

        bool check_storage = true;
        if (copy_job.job_status() == CopyJobPB::FINISH) {
            ++num_finished;

            if (copy_job.stage_type() == StagePB::INTERNAL) {
                auto it = stage_accessor_map.find(stage_id);
                std::shared_ptr<ObjStoreAccessor> accessor;
                if (it != stage_accessor_map.end()) {
                    accessor = it->second;
                } else {
                    auto ret = init_copy_job_accessor(stage_id, copy_job.stage_type(), &accessor);
                    if (ret < 0) { // error
                        return false;
                    } else if (ret == 0) {
                        stage_accessor_map.emplace(stage_id, accessor);
                    } else { // stage not found, skip check storage
                        check_storage = false;
                    }
                }
                if (check_storage) {
                    // delete objects on storage
                    std::vector<std::string> relative_paths;
                    for (const auto& file : copy_job.object_files()) {
                        auto relative_path = file.relative_path();
                        relative_paths.push_back(relative_path);
                        LOG_INFO("begin to delete internal stage object")
                                .tag("instance_id", instance_id_)
                                .tag("stage_id", stage_id)
                                .tag("table_id", table_id)
                                .tag("query_id", copy_id)
                                .tag("stage_path", accessor->path())
                                .tag("relative_path", relative_path);
                    }
                    LOG_INFO("begin to delete internal stage objects")
                            .tag("instance_id", instance_id_)
                            .tag("stage_id", stage_id)
                            .tag("table_id", table_id)
                            .tag("query_id", copy_id)
                            .tag("stage_path", accessor->path())
                            .tag("num_objects", relative_paths.size());

                    // TODO delete objects with key and etag is not supported
                    auto ret = accessor->delete_objects(relative_paths);
                    if (ret != 0) {
                        LOG_WARNING("failed to delete internal stage objects")
                                .tag("instance_id", instance_id_)
                                .tag("stage_id", stage_id)
                                .tag("table_id", table_id)
                                .tag("query_id", copy_id)
                                .tag("stage_path", accessor->path())
                                .tag("num_objects", relative_paths.size());
                        return -1;
                    }
                }
            } else if (copy_job.stage_type() == StagePB::EXTERNAL) {
                int64_t current_time =
                        duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
                if (copy_job.finish_time_ms() > 0) {
                    if (current_time <
                        copy_job.finish_time_ms() + config::copy_job_max_retention_second * 1000) {
                        return 0;
                    }
                } else {
                    // For compatibility, copy job does not contain finish time before 2.2.2, use start time
                    if (current_time <
                        copy_job.start_time_ms() + config::copy_job_max_retention_second * 1000) {
                        return 0;
                    }
                }
            }
        } else if (copy_job.job_status() == CopyJobPB::LOADING) {
            int64_t current_time =
                    duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
            // if copy job is timeout: delete all copy file kvs and copy job kv
            if (current_time <= copy_job.timeout_time_ms()) {
                return 0;
            }
            ++num_expired;
        }

        // delete all copy files
        std::vector<std::string> copy_file_keys;
        for (auto& file : copy_job.object_files()) {
            CopyFileKeyInfo file_key_info {instance_id_, stage_id, table_id, file.relative_path(),
                                           file.etag()};
            std::string file_key;
            copy_file_key(file_key_info, &file_key);
            copy_file_keys.push_back(std::move(file_key));
        }
        std::unique_ptr<Transaction> txn;
        if (txn_kv_->create_txn(&txn) != 0) {
            LOG(WARNING) << "failed to create txn";
            return -1;
        }
        // FIXME: We have already limited the file num and file meta size when selecting file in FE.
        // And if too many copy files, begin_copy failed commit too. So here the copy file keys are
        // limited, should not cause the txn commit failed.
        for (const auto& key : copy_file_keys) {
            txn->remove(key);
            LOG(INFO) << "remove copy_file_key=" << hex(key) << ", instance_id=" << instance_id_
                      << ", stage_id=" << stage_id << ", table_id=" << table_id
                      << ", query_id=" << copy_id;
        }
        txn->remove(k);
        if (txn->commit() != 0) {
            LOG(WARNING) << "failed to commit txn";
            return -1;
        }

        ++num_recycled;
        return 0;
    };

    scan_and_recycle(key0, key1, std::move(recycle_func));
}

int InstanceRecycler::init_copy_job_accessor(const std::string& stage_id,
                                             const StagePB::StageType& stage_type,
                                             std::shared_ptr<ObjStoreAccessor>* accessor) {
#ifdef UNIT_TEST
    // In unit test, external use the same accessor as the internal stage
    auto it = accessor_map_.find(stage_id);
    if (it != accessor_map_.end()) {
        *accessor = it->second;
    } else {
        std::cout << "UT can not find accessor with stage_id: " << stage_id << std::endl;
        return 1;
    }
#else
    // init s3 accessor and add to accessor map
    bool found = false;
    ObjectStoreInfoPB object_store_info;
    StagePB::StageAccessType stage_access_type = StagePB::AKSK;
    for (auto& s : instance_info_.stages()) {
        if (s.stage_id() == stage_id) {
            object_store_info = s.obj_info();
            if (s.has_access_type()) {
                stage_access_type = s.access_type();
            }
            found = true;
            break;
        }
    }
    if (!found) {
        LOG(INFO) << "Recycle nonexisted stage copy jobs. instance_id=" << instance_id_
                  << ", stage_id=" << stage_id << ", stage_type=" << stage_type;
        return 1;
    }
    S3Conf s3_conf;
    if (stage_type == StagePB::EXTERNAL) {
        s3_conf.endpoint = object_store_info.endpoint();
        s3_conf.region = object_store_info.region();
        s3_conf.bucket = object_store_info.bucket();
        s3_conf.prefix = object_store_info.prefix();
        if (stage_access_type == StagePB::AKSK) {
            s3_conf.ak = object_store_info.ak();
            s3_conf.sk = object_store_info.sk();
            if (object_store_info.has_encryption_info()) {
                AkSkPair plain_ak_sk_pair;
                int ret = decrypt_ak_sk_helper(object_store_info.ak(), object_store_info.sk(),
                                               object_store_info.encryption_info(),
                                               &plain_ak_sk_pair);
                if (ret != 0) {
                    LOG(WARNING) << "fail to decrypt ak sk. instance_id: " << instance_id_
                                 << " obj_info: " << proto_to_json(object_store_info);
                    return -1;
                }
                s3_conf.ak = std::move(plain_ak_sk_pair.first);
                s3_conf.sk = std::move(plain_ak_sk_pair.second);
            }
        } else if (stage_access_type == StagePB::BUCKET_ACL) {
            s3_conf.ak = instance_info_.ram_user().ak();
            s3_conf.sk = instance_info_.ram_user().sk();
            if (instance_info_.ram_user().has_encryption_info()) {
                AkSkPair plain_ak_sk_pair;
                int ret = decrypt_ak_sk_helper(
                        instance_info_.ram_user().ak(), instance_info_.ram_user().sk(),
                        instance_info_.ram_user().encryption_info(), &plain_ak_sk_pair);
                if (ret != 0) {
                    LOG(WARNING) << "fail to decrypt ak sk. instance_id: " << instance_id_
                                 << " ram_user: " << proto_to_json(instance_info_.ram_user());
                    return -1;
                }
                s3_conf.ak = std::move(plain_ak_sk_pair.first);
                s3_conf.sk = std::move(plain_ak_sk_pair.second);
            }
        } else {
            LOG(INFO) << "Unsupported stage access type=" << stage_access_type
                      << ", instance_id=" << instance_id_ << ", stage_id=" << stage_id;
            return -1;
        }
    } else if (stage_type == StagePB::INTERNAL) {
        int idx = stoi(object_store_info.id());
        if (idx > instance_info_.obj_info().size() || idx < 1) {
            LOG(WARNING) << "invalid idx: " << idx;
            return -1;
        }
        auto& old_obj = instance_info_.obj_info()[idx - 1];
        s3_conf.ak = old_obj.ak();
        s3_conf.sk = old_obj.sk();
        if (old_obj.has_encryption_info()) {
            AkSkPair plain_ak_sk_pair;
            int ret = decrypt_ak_sk_helper(old_obj.ak(), old_obj.sk(), old_obj.encryption_info(),
                                           &plain_ak_sk_pair);
            if (ret != 0) {
                LOG(WARNING) << "fail to decrypt ak sk. instance_id: " << instance_id_
                             << " obj_info: " << proto_to_json(old_obj);
                return -1;
            }
            s3_conf.ak = std::move(plain_ak_sk_pair.first);
            s3_conf.sk = std::move(plain_ak_sk_pair.second);
        }
        s3_conf.endpoint = old_obj.endpoint();
        s3_conf.region = old_obj.region();
        s3_conf.bucket = old_obj.bucket();
        s3_conf.prefix = object_store_info.prefix();
    } else {
        LOG(WARNING) << "unknown stage type " << stage_type;
        return -1;
    }
    *accessor = std::make_shared<S3Accessor>(std::move(s3_conf));
    auto ret = (*accessor)->init();
    if (ret != 0) {
        LOG(WARNING) << "failed to init s3 accessor ret=" << ret;
        return -1;
    }
#endif
    return 0;
}

void InstanceRecycler::recycle_stage() {
    int num_scanned = 0;
    int num_recycled = 0;

    LOG_INFO("begin to recycle stage").tag("instance_id", instance_id_);

    using namespace std::chrono;
    auto start_time = steady_clock::now();

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG_INFO("recycle stage, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_recycled", num_recycled);
    });

    RecycleStageKeyInfo key_info0 {instance_id_, ""};
    RecycleStageKeyInfo key_info1 {instance_id_, "\xff"};
    std::string key0;
    std::string key1;
    recycle_stage_key(key_info0, &key0);
    recycle_stage_key(key_info1, &key1);

    // Elements in `tmp_rowset_keys` has the same lifetime as `it`
    std::vector<std::string_view> stage_keys;
    auto recycle_func = [&num_scanned, &num_recycled, &stage_keys, this](
                                std::string_view k, std::string_view v) -> int {
        ++num_scanned;
        RecycleStagePB recycle_stage;
        if (!recycle_stage.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed recycle stage").tag("key", hex(k));
            return -1;
        }

        int idx = stoi(recycle_stage.stage().obj_info().id());
        if (idx > instance_info_.obj_info().size() || idx < 1) {
            LOG(WARNING) << "invalid idx: " << idx;
            return -1;
        }
        auto& old_obj = instance_info_.obj_info()[idx - 1];
        S3Conf s3_conf;
        s3_conf.ak = old_obj.ak();
        s3_conf.sk = old_obj.sk();
        if (old_obj.has_encryption_info()) {
            AkSkPair plain_ak_sk_pair;
            int ret = decrypt_ak_sk_helper(old_obj.ak(), old_obj.sk(), old_obj.encryption_info(),
                                           &plain_ak_sk_pair);
            if (ret != 0) {
                LOG(WARNING) << "fail to decrypt ak sk. instance_id: " << instance_id_
                             << " obj_info: " << proto_to_json(old_obj);
                return -1;
            }
            s3_conf.ak = std::move(plain_ak_sk_pair.first);
            s3_conf.sk = std::move(plain_ak_sk_pair.second);
        }
        s3_conf.endpoint = old_obj.endpoint();
        s3_conf.region = old_obj.region();
        s3_conf.bucket = old_obj.bucket();
        s3_conf.prefix = recycle_stage.stage().obj_info().prefix();
        std::shared_ptr<ObjStoreAccessor> accessor =
                std::make_shared<S3Accessor>(std::move(s3_conf));
        TEST_SYNC_POINT_CALLBACK("recycle_stage:get_accessor", &accessor);
        auto ret = accessor->init();
        if (ret != 0) {
            LOG(WARNING) << "failed to init s3 accessor ret=" << ret;
            return -1;
        }

        LOG_INFO("begin to delete objects of dropped internal stage")
                .tag("instance_id", instance_id_)
                .tag("stage_id", recycle_stage.stage().stage_id())
                .tag("user_name", recycle_stage.stage().mysql_user_name()[0])
                .tag("user_id", recycle_stage.stage().mysql_user_id()[0])
                .tag("obj_info_id", idx)
                .tag("prefix", recycle_stage.stage().obj_info().prefix());
        ret = accessor->delete_objects_by_prefix("");
        if (ret != 0) {
            LOG(WARNING) << "failed to delete objects of dropped internal stage. instance_id="
                         << instance_id_ << ", stage_id=" << recycle_stage.stage().stage_id()
                         << ", prefix=" << recycle_stage.stage().obj_info().prefix()
                         << ", ret=" << ret;
            return -1;
        }
        ++num_recycled;
        stage_keys.push_back(k);
        return 0;
    };

    auto loop_done = [&stage_keys, this]() -> int {
        if (stage_keys.empty()) return 0;
        std::unique_ptr<int, std::function<void(int*)>> defer((int*)0x01,
                                                              [&](int*) { stage_keys.clear(); });
        if (0 != txn_remove(txn_kv_.get(), stage_keys)) {
            LOG(WARNING) << "failed to delete recycle partition kv, instance_id=" << instance_id_;
            return -1;
        }
        return 0;
    };

    scan_and_recycle(key0, key1, std::move(recycle_func), std::move(loop_done));
}

void InstanceRecycler::recycle_expired_stage_objects() {
    LOG_INFO("begin to recycle expired stage objects").tag("instance_id", instance_id_);

    using namespace std::chrono;
    auto start_time = steady_clock::now();

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG_INFO("recycle expired stage objects, cost={}s", cost).tag("instance_id", instance_id_);
    });

    for (const auto& stage : instance_info_.stages()) {
        if (!is_working()) break;
        if (stage.type() == StagePB::EXTERNAL) {
            continue;
        }
        int idx = stoi(stage.obj_info().id());
        if (idx > instance_info_.obj_info().size() || idx < 1) {
            LOG(WARNING) << "invalid idx: " << idx << ", id: " << stage.obj_info().id();
            continue;
        }
        auto& old_obj = instance_info_.obj_info()[idx - 1];
        S3Conf s3_conf;
        s3_conf.ak = old_obj.ak();
        s3_conf.sk = old_obj.sk();
        if (old_obj.has_encryption_info()) {
            AkSkPair plain_ak_sk_pair;
            int ret = decrypt_ak_sk_helper(old_obj.ak(), old_obj.sk(), old_obj.encryption_info(),
                                           &plain_ak_sk_pair);
            if (ret != 0) {
                LOG(WARNING) << "fail to decrypt ak sk "
                             << "obj_info:" << proto_to_json(old_obj);
            } else {
                s3_conf.ak = std::move(plain_ak_sk_pair.first);
                s3_conf.sk = std::move(plain_ak_sk_pair.second);
            }
        }
        s3_conf.endpoint = old_obj.endpoint();
        s3_conf.region = old_obj.region();
        s3_conf.bucket = old_obj.bucket();
        s3_conf.prefix = stage.obj_info().prefix();
        std::shared_ptr<ObjStoreAccessor> accessor =
                std::make_shared<S3Accessor>(std::move(s3_conf));
        auto ret = accessor->init();
        if (ret != 0) {
            LOG(WARNING) << "failed to init s3 accessor ret=" << ret;
            continue;
        }

        LOG(INFO) << "recycle expired stage objects, instance_id=" << instance_id_
                  << ", stage_id=" << stage.stage_id()
                  << ", user_name=" << stage.mysql_user_name().at(0)
                  << ", user_id=" << stage.mysql_user_id().at(0)
                  << ", prefix=" << stage.obj_info().prefix();
        int64_t expired_time =
                duration_cast<seconds>(system_clock::now().time_since_epoch()).count() -
                config::internal_stage_objects_expire_time_second;
        ret = accessor->delete_expired_objects("", expired_time);
        if (ret != 0) {
            LOG(WARNING) << "failed to recycle expired stage objects, instance_id=" << instance_id_
                         << ", stage_id=" << stage.stage_id() << ", ret=" << ret;
            continue;
        }
    }
}
} // namespace selectdb
