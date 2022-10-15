#include "recycler/recycler.h"

#include <gen_cpp/olap_file.pb.h>

#include <atomic>
#include <chrono>
#include <string>
#include <string_view>

#include "../test/mock_accessor.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/util.h"
#include "meta-service/keys.h"
#include "recycler/recycler_service.h"

namespace selectdb {

static std::atomic_bool s_is_working = false;

static bool is_working() {
    return s_is_working.load(std::memory_order_acquire);
}

Recycler::Recycler() : server_(new brpc::Server()) {};

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

            LOG(INFO) << "range get instance_key=" << hex(k);

            InstanceInfoPB instance_info;
            if (!instance_info.ParseFromArray(v.data(), v.size())) {
                LOG_WARNING("malformed instance info").tag("key", hex(k)).tag("val", hex(v));
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
    std::lock_guard lock(pending_instance_queue_mtx_);
    for (auto&& instance : instances) {
        pending_instance_queue_.push_back(std::move(instance));
    }
    pending_instance_queue_cond_.notify_all();
}

void Recycler::instance_scanner_callback() {
    while (is_working()) {
        auto instances = get_instances();
        add_pending_instances(std::move(instances));
        {
            std::unique_lock lock(instance_scanner_mtx_);
            instance_scanner_cond_.wait_for(lock,
                                            std::chrono::seconds(config::recycl_interval_seconds),
                                            [&]() { return !is_working(); });
        }
    }
}

void Recycler::recycle_callback() {
    while (is_working()) {
        InstanceInfoPB instance;
        {
            std::unique_lock lock(pending_instance_queue_mtx_);
            pending_instance_queue_cond_.wait(
                    lock, [&]() { return !pending_instance_queue_.empty() || !is_working(); });
            if (!is_working()) {
                return;
            }
            instance = std::move(pending_instance_queue_.front());
            pending_instance_queue_.pop_front();
        }
        {
            std::lock_guard lock(recycling_instance_set_mtx_);
            auto [_, success] = recycling_instance_set_.insert(instance.instance_id());
            if (!success) {
                // skip instance in recycling
                continue;
            }
        }
        auto& instance_id = instance.instance_id();
        if (instance.obj_info().empty()) {
            LOG_WARNING("instance has no object store info").tag("instance_id", instance_id);
            continue;
        }
        auto instance_recycler = std::make_unique<InstanceRecycler>(txn_kv_, instance);
        LOG_INFO("begin to recycle instance").tag("instance_id", instance_id);
        instance_recycler->recycle_indexes();
        instance_recycler->recycle_partitions();
        instance_recycler->recycle_tmp_rowsets();
        instance_recycler->recycle_rowsets();
        instance_recycler->abort_timeout_txn();
        instance_recycler->recycle_expired_txn_label();
        {
            std::lock_guard lock(recycling_instance_set_mtx_);
            recycling_instance_set_.erase(instance_id);
        }
        LOG_INFO("finish recycle instance").tag("instance_id", instance_id);
    }
}

int Recycler::start() {
    int ret = 0;
    txn_kv_ = std::make_shared<FdbTxnKv>();
    LOG(INFO) << "begin to init txn kv";
    ret = txn_kv_->init();
    if (ret != 0) {
        LOG(WARNING) << "failed to init txnkv, ret=" << ret;
        return 1;
    }
    LOG(INFO) << "successfully init txn kv";

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

    s_is_working = true;

    if (config::recycle_standalone_mode) {
        workers_.push_back(std::thread(std::bind(&Recycler::instance_scanner_callback, this)));
    }
    for (int i = 0; i < config::recycle_concurrency; ++i) {
        workers_.push_back(std::thread(std::bind(&Recycler::recycle_callback, this)));
    }
    return 0;
}

void Recycler::join() {
    server_->RunUntilAskedToQuit();
    server_->ClearServices();

    s_is_working = false;

    instance_scanner_cond_.notify_all();
    pending_instance_queue_cond_.notify_all();
    for (auto& w : workers_) {
        w.join();
    }
}

InstanceRecycler::InstanceRecycler(std::shared_ptr<TxnKv> txn_kv, const InstanceInfoPB& instance)
        : txn_kv_(std::move(txn_kv)), instance_id_(instance.instance_id()) {
    for (auto& obj_info : instance.obj_info()) {
        S3Conf s3_conf;
        s3_conf.ak = obj_info.ak();
        s3_conf.sk = obj_info.sk();
        s3_conf.endpoint = obj_info.endpoint();
        s3_conf.region = obj_info.region();
        s3_conf.bucket = obj_info.bucket();
        s3_conf.prefix = obj_info.prefix();
#ifdef UNIT_TEST
        auto accessor = std::make_shared<MockAccessor>(s3_conf);
#else
        auto accessor = std::make_shared<S3Accessor>(std::move(s3_conf));
#endif
        accessor->init();
        accessor_map_.emplace(obj_info.id(), std::move(accessor));
    }
}

void InstanceRecycler::recycle_indexes() {
    int num_scanned = 0;
    int num_expired = 0;
    int num_recycled = 0;

    RecycleIndexKeyInfo index_key_info0 {instance_id_, 0};
    RecycleIndexKeyInfo index_key_info1 {instance_id_, std::numeric_limits<int64_t>::max()};
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

    int64_t current_time = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();

    auto recycle_func = [&num_scanned, &num_expired, &num_recycled, current_time, this](
                                std::string_view k, std::string_view v) -> bool {
        ++num_scanned;
        RecycleIndexPB index_pb;
        if (!index_pb.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed recycle index value").tag("key", hex(k)).tag("val", hex(v));
            return false;
        }
        if (current_time < index_pb.creation_time() + config::index_retention_seconds) {
            // not expired
            return false;
        }
        ++num_expired;
        // decode index_id
        auto k1 = k;
        k1.remove_prefix(1);
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        decode_key(&k1, &out);
        // 0x01 "recycle" ${instance_id} "index" ${index_id} -> RecycleIndexPB
        auto index_id = std::get<int64_t>(std::get<0>(out[3]));
        if (recycle_tablets(index_pb.table_id(), index_id) != 0) {
            LOG_WARNING("failed to recycle tablets under index")
                    .tag("table_id", index_pb.table_id())
                    .tag("instance_id", instance_id_)
                    .tag("index_id", index_id);
            return false;
        }
        ++num_recycled;
        return true;
    };

    scan_and_recycle(index_key0, index_key1, std::move(recycle_func));
}

void InstanceRecycler::recycle_partitions() {
    int num_scanned = 0;
    int num_expired = 0;
    int num_recycled = 0;

    RecyclePartKeyInfo part_key_info0 {instance_id_, 0};
    RecyclePartKeyInfo part_key_info1 {instance_id_, std::numeric_limits<int64_t>::max()};
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

    int64_t current_time = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();

    auto recycle_func = [&num_scanned, &num_expired, &num_recycled, current_time, this](
                                std::string_view k, std::string_view v) -> bool {
        ++num_scanned;
        RecyclePartitionPB part_pb;
        if (!part_pb.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed recycle partition value").tag("key", hex(k)).tag("val", hex(v));
            return false;
        }
        if (current_time < part_pb.creation_time() + config::partition_retention_seconds) {
            // not expired
            return false;
        }
        ++num_expired;
        // decode partition_id
        auto k1 = k;
        k1.remove_prefix(1);
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        decode_key(&k1, &out);
        // 0x01 "recycle" ${instance_id} "partition" ${partition_id} -> RecyclePartitionPB
        auto partition_id = std::get<int64_t>(std::get<0>(out[3]));
        bool success = true;
        for (int64_t index_id : part_pb.index_id()) {
            if (recycle_tablets(part_pb.table_id(), index_id, partition_id) != 0) {
                LOG_WARNING("failed to recycle tablets under partition")
                        .tag("table_id", part_pb.table_id())
                        .tag("instance_id", instance_id_)
                        .tag("index_id", index_id)
                        .tag("partition_id", partition_id);
                success = false;
            }
        }
        if (success) {
            ++num_recycled;
        }
        return success;
    };

    scan_and_recycle(part_key0, part_key1, std::move(recycle_func));
}

int InstanceRecycler::recycle_tablets(int64_t table_id, int64_t index_id, int64_t partition_id) {
    int num_scanned = 0;
    int num_recycled = 0;

    std::string tablet_key0;
    std::string tablet_key1;
    if (partition_id > 0) {
        // recycle tablets in a partition belonging to the index
        MetaTabletKeyInfo tablet_key_info0 {instance_id_, table_id, index_id, partition_id, 0};
        MetaTabletKeyInfo tablet_key_info1 {instance_id_, table_id, index_id, partition_id + 1, 0};
        meta_tablet_key(tablet_key_info0, &tablet_key0);
        meta_tablet_key(tablet_key_info1, &tablet_key1);
    } else {
        // recycle tablets in the index
        MetaTabletKeyInfo tablet_key_info0 {instance_id_, table_id, index_id, 0, 0};
        MetaTabletKeyInfo tablet_key_info1 {instance_id_, table_id, index_id + 1, 0, 0};
        meta_tablet_key(tablet_key_info0, &tablet_key0);
        meta_tablet_key(tablet_key_info1, &tablet_key1);
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

    auto recycle_func = [&num_scanned, &num_recycled, this](std::string_view k,
                                                            std::string_view v) -> bool {
        ++num_scanned;
        doris::TabletMetaPB tablet_meta_pb;
        if (!tablet_meta_pb.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed tablet meta").tag("key", hex(k)).tag("val", hex(v));
            return false;
        }
        if (recycle_tablet(tablet_meta_pb.tablet_id()) != 0) {
            LOG_WARNING("failed to recycle tablet")
                    .tag("instance_id", instance_id_)
                    .tag("tablet_id", tablet_meta_pb.tablet_id());
            return false;
        }
        ++num_recycled;
        return true;
    };

    return scan_and_recycle(tablet_key0, tablet_key1, std::move(recycle_func), true);
}

int InstanceRecycler::delete_rowset_data(const doris::RowsetMetaPB& rs_meta_pb) {
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
    int64_t num_segments = rs_meta_pb.num_segments();
    std::vector<std::string> segment_paths;
    segment_paths.reserve(num_segments);
    for (int64_t i = 0; i < num_segments; ++i) {
        segment_paths.push_back(fmt::format("data/{}/{}_{}.dat", tablet_id, rowset_id, i));
    }
    LOG_INFO("begin to delete rowset data")
            .tag("s3_path", accessor->path())
            .tag("tablet_id", tablet_id)
            .tag("rowset_id", rowset_id)
            .tag("num_segments", num_segments);
    int ret = accessor->delete_objects(segment_paths);
    if (ret != 0) {
        LOG_INFO("failed to delete rowset data")
                .tag("s3_path", accessor->path())
                .tag("tablet_id", tablet_id)
                .tag("rowset_id", rowset_id)
                .tag("num_segments", num_segments);
    }
    return ret;
}

int InstanceRecycler::delete_rowset_data(const std::string& rowset_id,
                                         const RecycleRowsetPB& recycl_rs_pb) {
    auto it = accessor_map_.find(recycl_rs_pb.resource_id());
    if (it == accessor_map_.end()) {
        LOG_WARNING("instance has no such resource id")
                .tag("instance_id", instance_id_)
                .tag("resource_id", recycl_rs_pb.resource_id());
        return -1;
    }
    auto& accessor = it->second;
    LOG_INFO("begin to delete rowset data")
            .tag("s3_path", accessor->path())
            .tag("tablet_id", recycl_rs_pb.tablet_id())
            .tag("rowset_id", rowset_id);
    int ret = accessor->delete_objects_by_prefix(
            fmt::format("data/{}/{}", recycl_rs_pb.tablet_id(), rowset_id));
    if (ret != 0) {
        LOG_INFO("failed to delete rowset data")
                .tag("s3_path", accessor->path())
                .tag("tablet_id", recycl_rs_pb.tablet_id())
                .tag("rowset_id", rowset_id);
    }
    return ret;
}

int InstanceRecycler::recycle_tablet(int64_t tablet_id) {
    int num_scanned = 0;
    int num_recycled = 0;

    LOG_INFO("begin to recycle rowsets in a dropped tablet")
            .tag("instance_id", instance_id_)
            .tag("tablet_id", tablet_id);

    using namespace std::chrono;
    auto start_time = steady_clock::now();

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG_INFO("recycle rowsets finished, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("tablet_id", tablet_id)
                .tag("num_scanned", num_scanned)
                .tag("num_recycled", num_recycled);
    });

    // recycle committed rowsets in the tablet
    MetaRowsetKeyInfo rs_key_info0 {instance_id_, tablet_id, 0};
    MetaRowsetKeyInfo rs_key_info1 {instance_id_, tablet_id + 1, 0};
    std::string rs_key0;
    std::string rs_key1;
    meta_rowset_key(rs_key_info0, &rs_key0);
    meta_rowset_key(rs_key_info1, &rs_key1);

    auto recycle_committed_rowset = [&num_scanned, &num_recycled, this](
                                            std::string_view k, std::string_view v) -> bool {
        ++num_scanned;
        doris::RowsetMetaPB rs_meta_pb;
        if (!rs_meta_pb.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed rowset meta").tag("key", hex(k)).tag("val", hex(v));
            return false;
        }
        if (!rs_meta_pb.has_resource_id()) {
            if (rs_meta_pb.data_disk_size() != 0) {
                LOG_WARNING("rowset meta has empty resource id")
                        .tag("instance_id", instance_id_)
                        .tag("rowset_id", rs_meta_pb.rowset_id_v2());
            }
            ++num_recycled;
            return true;
        }
        int ret = delete_rowset_data(rs_meta_pb);
        if (ret != 0) {
            return false;
        }
        ++num_recycled;
        return true;
    };

    int ret = scan_and_recycle(rs_key0, rs_key1, std::move(recycle_committed_rowset), true);
    if (ret != 0) {
        return ret;
    }

    // recycle prepared rowsets in the tablet,
    // ignore any error as these rowsets will eventually be recycled by `recycle_rowsets`
    RecycleRowsetKeyInfo recyc_rs_key_info0 {instance_id_, tablet_id, ""};
    RecycleRowsetKeyInfo recyc_rs_key_info1 {instance_id_, tablet_id + 1, ""};
    std::string recyc_rs_key0;
    std::string recyc_rs_key1;
    recycle_rowset_key(recyc_rs_key_info0, &recyc_rs_key0);
    recycle_rowset_key(recyc_rs_key_info1, &recyc_rs_key1);

    auto recycle_prepared_rowset = [&num_scanned, &num_recycled, this](std::string_view k,
                                                                       std::string_view v) -> bool {
        ++num_scanned;
        RecycleRowsetPB recyc_rs_pb;
        if (!recyc_rs_pb.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed recycle rowset value").tag("key", hex(k)).tag("val", hex(v));
            return false;
        }
        // decode rowset_id
        auto k1 = k;
        k1.remove_prefix(1);
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        decode_key(&k1, &out);
        // 0x01 "recycle" ${instance_id} "rowset" ${tablet_id} ${rowset_id} -> RecycleRowsetPB
        const auto& rowset_id = std::get<std::string>(std::get<0>(out[4]));
        if (!recyc_rs_pb.has_resource_id()) {
            LOG_WARNING("rowset meta has empty resource id")
                    .tag("instance_id", instance_id_)
                    .tag("rowset_id", rowset_id);
            ++num_recycled;
            return true;
        }
        int ret = delete_rowset_data(rowset_id, recyc_rs_pb);
        if (ret != 0) {
            return false;
        }
        ++num_recycled;
        return true;
    };

    // Ignore the errors when recycling prepared rowsets in tablet,
    // as these rowsets will be recycled by `recycle_rowsets` soon.
    scan_and_recycle(recyc_rs_key0, recyc_rs_key1, std::move(recycle_prepared_rowset), true);

    return 0;
}

void InstanceRecycler::recycle_rowsets() {
    int num_scanned = 0;
    int num_expired = 0;
    int num_recycled = 0;

    RecycleRowsetKeyInfo recyc_rs_key_info0 {instance_id_, 0, ""};
    RecycleRowsetKeyInfo recyc_rs_key_info1 {instance_id_, std::numeric_limits<int64_t>::max(), ""};
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

    int64_t current_time = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();

    auto recycle_func = [&num_scanned, &num_expired, &num_recycled, current_time, this](
                                std::string_view k, std::string_view v) -> bool {
        ++num_scanned;
        RecycleRowsetPB recyc_rs_pb;
        if (!recyc_rs_pb.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed recycle rowset value").tag("key", hex(k)).tag("val", hex(v));
            return false;
        }
        if (current_time < recyc_rs_pb.creation_time() + config::rowset_retention_seconds) {
            // not expired
            return false;
        }
        ++num_expired;
        // decode rowset_id
        auto k1 = k;
        k1.remove_prefix(1);
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        decode_key(&k1, &out);
        // 0x01 "recycle" ${instance_id} "rowset" ${tablet_id} ${rowset_id} -> RecycleRowsetPB
        const auto& rowset_id = std::get<std::string>(std::get<0>(out[4]));
        if (!recyc_rs_pb.has_resource_id()) {
            LOG_WARNING("rowset meta has empty resource id")
                    .tag("instance_id", instance_id_)
                    .tag("rowset_id", rowset_id);
            ++num_recycled;
            return true;
        }
        int ret = delete_rowset_data(rowset_id, recyc_rs_pb);
        if (ret != 0) {
            return false;
        }
        ++num_recycled;
        return true;
    };

    scan_and_recycle(recyc_rs_key0, recyc_rs_key1, std::move(recycle_func));
}

void InstanceRecycler::recycle_tmp_rowsets() {
    int num_scanned = 0;
    int num_expired = 0;
    int num_recycled = 0;

    MetaRowsetTmpKeyInfo tmp_rs_key_info0 {instance_id_, 0, 0};
    MetaRowsetTmpKeyInfo tmp_rs_key_info1 {instance_id_, std::numeric_limits<int64_t>::max(), 0};
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

    int64_t current_time = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();

    auto recycle_func = [&num_scanned, &num_expired, &num_recycled, current_time, this](
                                std::string_view k, std::string_view v) -> bool {
        ++num_scanned;
        doris::RowsetMetaPB rs_meta_pb;
        if (!rs_meta_pb.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed rowset meta").tag("key", hex(k)).tag("val", hex(v));
            return false;
        }
        if (current_time < rs_meta_pb.creation_time() + config::rowset_retention_seconds) {
            // not expired
            return false;
        }
        ++num_expired;
        if (!rs_meta_pb.has_resource_id()) {
            if (rs_meta_pb.data_disk_size() != 0) {
                LOG_WARNING("rowset meta has empty resource id")
                        .tag("instance_id", instance_id_)
                        .tag("rowset_id", rs_meta_pb.rowset_id_v2());
            }
            ++num_recycled;
            return true;
        }
        int ret = delete_rowset_data(rs_meta_pb);
        if (ret != 0) {
            return false;
        }
        ++num_recycled;
        return true;
    };

    scan_and_recycle(tmp_rs_key0, tmp_rs_key1, std::move(recycle_func));
}

int InstanceRecycler::scan_and_recycle(
        std::string begin, std::string_view end,
        std::function<bool(std::string_view k, std::string_view v)> recycle_func,
        bool try_range_remove_kv) {
    int ret = 0;
    std::unique_ptr<RangeGetIterator> it;
    // elements in `recycled_keys` has the same lifetime as `it`
    std::vector<std::string_view> recycled_keys;
    do {
        recycled_keys.clear();
        {
            // scan kvs
            std::unique_ptr<Transaction> txn;
            if (txn_kv_->create_txn(&txn) != 0) {
                LOG(WARNING) << "failed to create txn";
                return -1;
            }
            if (txn->get(begin, end, &it) != 0) {
                LOG(WARNING) << "failed to get kv";
                return -1;
            }
            VLOG_DEBUG << "fetch " << it->size() << " kv";
        }
        if (!it->has_next()) {
            LOG_INFO("no keys in the given range").tag("begin", hex(begin)).tag("end", hex(end));
            break;
        }
        while (it->has_next()) {
            // recycle corresponding resources
            auto [k, v] = it->next();
            if (!it->has_next()) {
                begin = k;
                LOG_INFO("iterator has no more kvs")
                        .tag("last_key", hex(k))
                        .tag("last_val_size", v.size());
            }
            if (recycle_func(k, v)) {
                recycled_keys.push_back(k);
            } else {
                ret = -1;
            }
        }
        begin.push_back('\x00'); // Update to next smallest key for iteration
        if (!recycled_keys.empty()) {
            // remove recycled kvs
            std::unique_ptr<Transaction> txn;
            if (txn_kv_->create_txn(&txn) != 0) {
                LOG(WARNING) << "failed to create txn";
                ret = -1;
                continue;
            }
            if (try_range_remove_kv && recycled_keys.size() == it->size()) {
                txn->remove(recycled_keys.front(), begin);
            } else {
                for (auto k : recycled_keys) {
                    txn->remove(k);
                }
            }
            if (txn->commit() != 0) {
                LOG(WARNING) << "failed to commit txn";
                ret = -1;
                continue;
            }
        }
    } while (it->more() && is_working());
    return ret;
}

void InstanceRecycler::abort_timeout_txn() {
    int ret = 0;
    int num_scanned = 0;
    int num_timeout = 0;
    int num_abort = 0;

    TxnRunningKeyInfo txn_running_key_info0 {instance_id_, 0, 0};
    TxnRunningKeyInfo txn_running_key_info1 {instance_id_, std::numeric_limits<int64_t>::max(),
                                             std::numeric_limits<int64_t>::max()};
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
    VLOG_DEBUG << "current_time=" << current_time;

    std::unique_ptr<RangeGetIterator> it;
    std::vector<std::string_view> timeout_txn_running_keys;
    do {
        timeout_txn_running_keys.clear();
        {
            std::unique_ptr<Transaction> txn;
            ret = txn_kv_->create_txn(&txn);
            if (ret != 0) {
                LOG(ERROR) << "failed to create txn ret=" << ret;
                return;
            }
            ret = txn->get(begin_txn_running_key, end_txn_running_key, &it,
                           config::expired_txn_scan_key_nums);
            if (ret != 0) {
                LOG_WARNING("failed to get kv range ret={}", ret)
                        .tag("begin", hex(begin_txn_running_key))
                        .tag("end", hex(end_txn_running_key));
                return;
            }
            VLOG_DEBUG << "fetch " << it->size() << " kv, begin=" << hex(begin_txn_running_key)
                       << " end=" << hex(end_txn_running_key);
        }

        while (it->has_next()) {
            auto [k, v] = it->next();
            VLOG_DEBUG << "k=" << hex(k) << " v=" << hex(v);

            if (!it->has_next()) {
                begin_txn_running_key = k;
                LOG_INFO("iterator has no more kvs")
                        .tag("last_key", hex(k))
                        .tag("last_val_size", v.size());
            }

            ++num_scanned;
            TxnRunningPB txn_running_pb;
            if (!txn_running_pb.ParseFromArray(v.data(), v.size())) {
                LOG_WARNING("malformed txn_running_pb").tag("key", hex(k)).tag("val", hex(v));
                continue;
            }
            VLOG_DEBUG << "txn_running_pb:" << txn_running_pb.DebugString();
            if (txn_running_pb.timeout_time() <= current_time) {
                LOG_INFO("found timeout txn").tag("key", hex(k));
                timeout_txn_running_keys.push_back(k);
                num_timeout++;
            }
        }

        begin_txn_running_key.push_back('\x00');
        {
            for (const auto& k : timeout_txn_running_keys) {
                //abort timeout txn
                std::unique_ptr<Transaction> txn;
                ret = txn_kv_->create_txn(&txn);
                if (ret != 0) {
                    LOG_ERROR("failed to create txn ret={}", ret).tag("key", hex(k));
                    continue;
                }

                std::string_view k1 = k;
                //TxnRunningKeyInfo 0:instance_id  1:db_id  2:txn_id
                k1.remove_prefix(1); // Remove key space
                std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
                ret = decode_key(&k1, &out);
                if (ret != 0) {
                    LOG_ERROR("failed to decode key, ret={}", ret).tag("key", hex(k));
                    continue;
                }

                int64_t db_id = std::get<int64_t>(std::get<0>(out[3]));
                int64_t txn_id = std::get<int64_t>(std::get<0>(out[4]));
                VLOG_DEBUG << "instance_id=" << instance_id_ << " db_id=" << db_id
                           << " txn_id=" << txn_id;

                std::string txn_inf_key;
                std::string txn_inf_val;
                // Get txn info with db_id and txn_id
                TxnInfoKeyInfo txn_inf_key_info {instance_id_, db_id, txn_id};
                txn_info_key(txn_inf_key_info, &txn_inf_key);
                ret = txn->get(txn_inf_key, &txn_inf_val);
                if (ret != 0) {
                    LOG_WARNING("failed to get txn info ret={}, txn_inf_key={}", ret,
                                hex(txn_inf_key))
                            .tag("key", hex(k))
                            .tag("db_id", db_id)
                            .tag("txn_id", txn_id);
                    continue;
                }

                TxnInfoPB txn_info;
                if (!txn_info.ParseFromString(txn_inf_val)) {
                    LOG_WARNING("failed to parse txn info")
                            .tag("key", hex(k))
                            .tag("db_id", db_id)
                            .tag("txn_id", txn_id);
                    continue;
                }

                // Update txn_info
                txn_info.set_status(TxnStatusPB::TXN_STATUS_ABORTED);
                txn_info.set_finish_time(current_time);
                txn_info.set_reason("timeout");
                VLOG_DEBUG << "txn_info=" << txn_info.DebugString();

                txn_inf_val.clear();
                if (!txn_info.SerializeToString(&txn_inf_val)) {
                    LOG_WARNING("failed to serialize txn info")
                            .tag("key", hex(k))
                            .tag("db_id", db_id)
                            .tag("txn_id", txn_id);
                    continue;
                }
                txn->put(txn_inf_key, txn_inf_val);
                VLOG_DEBUG << "txn->put, txn_inf_key=" << hex(txn_inf_key);

                std::string txn_run_key;
                TxnRunningKeyInfo txn_run_key_info {instance_id_, db_id, txn_id};
                txn_running_key(txn_run_key_info, &txn_run_key);
                txn->remove(txn_run_key);
                VLOG_DEBUG << "txn->remove, txn_run_key=" << hex(txn_run_key);

                std::string recycle_txn_key_;
                std::string recycle_txn_val;
                RecycleTxnKeyInfo recycle_txn_key_info {instance_id_, db_id, txn_id};

                recycle_txn_key(recycle_txn_key_info, &recycle_txn_key_);
                RecycleTxnPB recycle_txn_pb;
                recycle_txn_pb.set_creation_time(current_time);
                recycle_txn_pb.set_label(txn_info.label());

                if (!recycle_txn_pb.SerializeToString(&recycle_txn_val)) {
                    LOG_WARNING("failed to serialize txn recycle info")
                            .tag("key", hex(k))
                            .tag("db_id", db_id)
                            .tag("txn_id", txn_id);
                    continue;
                }
                txn->put(recycle_txn_key_, recycle_txn_val);
                VLOG_DEBUG << "txn->put, recycle_txn_key_=" << hex(recycle_txn_key_);

                ret = txn->commit();
                if (ret != 0) {
                    LOG_WARNING("failed to commit txn ret={}", ret)
                            .tag("key", hex(k))
                            .tag("db_id", db_id)
                            .tag("txn_id", txn_id);
                    continue;
                }
                num_abort++;
                LOG_INFO("abort timeout txn")
                        .tag("key", hex(k))
                        .tag("db_id", db_id)
                        .tag("txn_id", txn_id)
                        .tag("label", txn_info.label());
            }
        }
    } while (it->more());

    return;
}

void InstanceRecycler::recycle_expired_txn_label() {
    int ret = 0;
    int num_scanned = 0;
    int num_expired = 0;
    int num_recycle = 0;

    RecycleTxnKeyInfo recycle_txn_key_info0 {instance_id_, 0, 0};
    RecycleTxnKeyInfo recycle_txn_key_info1 {instance_id_, std::numeric_limits<int64_t>::max(),
                                             std::numeric_limits<int64_t>::max()};
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
                .tag("num_recycle", num_recycle);
    });

    int64_t current_time =
            duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    VLOG_DEBUG << "current_time=" << current_time;

    std::unique_ptr<RangeGetIterator> it;
    std::vector<std::string_view> expire_recycle_txn_keys;
    do {
        expire_recycle_txn_keys.clear();
        {
            std::unique_ptr<Transaction> txn;
            ret = txn_kv_->create_txn(&txn);
            if (ret != 0) {
                LOG(ERROR) << "failed to create txn"
                           << " ret=" << ret;
                return;
            }
            ret = txn->get(begin_recycle_txn_key, end_recycle_txn_key, &it,
                           config::expired_txn_scan_key_nums);
            if (ret != 0) {
                LOG_WARNING("failed to get kv range ret={}", ret)
                        .tag("begin", hex(begin_recycle_txn_key))
                        .tag("end", hex(end_recycle_txn_key));
                return;
            }
            VLOG_DEBUG << "fetch " << it->size() << " kv, begin=" << hex(begin_recycle_txn_key)
                       << " end=" << hex(end_recycle_txn_key);
        }

        while (it->has_next()) {
            auto [k, v] = it->next();
            VLOG_DEBUG << "k=" << hex(k) << " v=" << hex(v);

            if (!it->has_next()) {
                begin_recycle_txn_key = k;
                LOG_INFO("iterator has no more kvs")
                        .tag("last_key", hex(k))
                        .tag("last_val_size", v.size());
            }

            ++num_scanned;
            RecycleTxnPB recycle_txn_pb;
            if (!recycle_txn_pb.ParseFromArray(v.data(), v.size())) {
                LOG_WARNING("malformed txn_running_pb").tag("key", hex(k)).tag("val", hex(v));
                continue;
            }

            VLOG_DEBUG << "recycle_txn_pb:" << recycle_txn_pb.DebugString();
            if ((recycle_txn_pb.has_immediate() && recycle_txn_pb.immediate()) ||
                (recycle_txn_pb.creation_time() + config::label_keep_max_second <= current_time)) {
                LOG_INFO("found recycle txn").tag("key", hex(k));
                expire_recycle_txn_keys.push_back(k);
                num_expired++;
            }
        }

        begin_recycle_txn_key.push_back('\x00');

        {
            for (const auto& k : expire_recycle_txn_keys) {
                //recycle expired txn
                std::unique_ptr<Transaction> txn;
                ret = txn_kv_->create_txn(&txn);
                if (ret != 0) {
                    LOG_ERROR("failed to create txn ret={}", ret).tag("key", hex(k));
                    continue;
                }

                std::string_view k1 = k;
                //RecycleTxnKeyInfo 0:instance_id  1:db_id  2:txn_id
                k1.remove_prefix(1); // Remove key space
                std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
                ret = decode_key(&k1, &out);
                if (ret != 0) {
                    LOG_ERROR("failed to decode key, ret={}", ret).tag("key", hex(k));
                    continue;
                }
                int64_t db_id = std::get<int64_t>(std::get<0>(out[3]));
                int64_t txn_id = std::get<int64_t>(std::get<0>(out[4]));
                VLOG_DEBUG << "instance_id=" << instance_id_ << " db_id=" << db_id
                           << " txn_id=" << txn_id;

                std::string txn_index_key_;
                TxnIndexKeyInfo txn_index_key_info {instance_id_, txn_id};
                txn_index_key(txn_index_key_info, &txn_index_key_);

                std::string txn_inf_key;
                std::string txn_inf_val;
                // Get txn info with db_id and txn_id
                TxnInfoKeyInfo txn_inf_key_info {instance_id_, db_id, txn_id};
                txn_info_key(txn_inf_key_info, &txn_inf_key);
                ret = txn->get(txn_inf_key, &txn_inf_val);
                if (ret != 0) {
                    LOG_WARNING("failed to get txn info ret={}, txn_inf_key={}", ret,
                                hex(txn_inf_key))
                            .tag("key", hex(k))
                            .tag("db_id", db_id)
                            .tag("txn_id", txn_id);
                    continue;
                }

                TxnInfoPB txn_info;
                if (!txn_info.ParseFromString(txn_inf_val)) {
                    LOG_WARNING("failed to parse txn info")
                            .tag("key", hex(k))
                            .tag("db_id", db_id)
                            .tag("txn_id", txn_id);
                    continue;
                }
                VLOG_DEBUG << "txn_info:" << txn_info.DebugString();

                std::string txn_label_key_;
                std::string txn_label_val;
                TxnLabelKeyInfo txn_label_key_info {instance_id_, db_id, txn_info.label()};
                txn_label_key(txn_label_key_info, &txn_label_key_);

                ret = txn->get(txn_label_key_, &txn_label_val);
                if (ret != 0) {
                    continue;
                }

                VLOG_DEBUG << "txn->get txn_label_key=" << hex(txn_label_key_)
                           << " txn_label_val=" << hex(txn_label_key_);
                std::string txn_label_pb_str =
                        txn_label_val.substr(0, txn_label_val.size() - VERSION_STAMP_LEN);

                TxnLabelPB txn_label_pb;
                if (!txn_label_pb.ParseFromString(txn_label_pb_str)) {
                    LOG_WARNING("failed to parse txn index")
                            .tag("key", hex(k))
                            .tag("db_id", db_id)
                            .tag("txn_id", txn_id);
                    continue;
                }

                VLOG_DEBUG << "txn_label_pb:" << txn_label_pb.DebugString();

                auto iter = txn_label_pb.txn_ids().begin();
                for (int64_t cur_txn_id : txn_label_pb.txn_ids()) {
                    if (cur_txn_id == txn_id) {
                        break;
                    }
                    iter++;
                }
                txn_label_pb.mutable_txn_ids()->erase(iter);

                if (txn_label_pb.txn_ids().empty()) {
                    txn->remove(txn_label_key_);
                    VLOG_DEBUG << "txn->remove, txn_label_key=" << hex(txn_label_key_)
                               << " label=" << txn_info.label() << " txn_id=" << txn_id;
                } else {
                    if (!txn_label_pb.SerializeToString(&txn_label_val)) {
                        LOG_WARNING("failed to serialize txn index label={}", txn_info.label())
                                .tag("key", hex(k))
                                .tag("db_id", db_id)
                                .tag("txn_id", txn_id);
                        continue;
                    }

                    txn->atomic_set_ver_value(txn_label_key_, txn_label_val);
                    VLOG_DEBUG << "update txn_label_key=" << hex(txn_label_key_)
                               << " label=" << txn_info.label() << " txn_id=" << txn_id
                               << " txn_label_pb:" << txn_label_pb.DebugString();
                }

                txn->remove(k);
                VLOG_DEBUG << "txn->remove, recycle k=" << hex(k);
                txn->remove(txn_inf_key);
                VLOG_DEBUG << "txn->remove, txn_inf_key=" << hex(txn_inf_key);
                txn->remove(txn_index_key_);
                VLOG_DEBUG << "txn->remove, txn_index_key=" << hex(txn_index_key_);

                ret = txn->commit();
                if (ret != 0) {
                    LOG_WARNING("failed to commit txn ret={}", ret)
                            .tag("key", hex(k))
                            .tag("db_id", db_id)
                            .tag("txn_id", txn_id);
                    continue;
                }
                num_recycle++;
                LOG_INFO("recycle expired txn")
                        .tag("key", hex(k))
                        .tag("db_id", db_id)
                        .tag("txn_id", txn_id);
            }
        }
    } while (it->more());

    return;
}
} // namespace selectdb
