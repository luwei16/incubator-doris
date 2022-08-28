#include "recycler/recycler.h"

#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/selectdb_cloud.pb.h>

#include <chrono>
#include <sstream>
#include <string>
#include <string_view>

#include "common/config.h"
#include "common/logging.h"
#include "common/util.h"
#include "meta-service/keys.h"

namespace selectdb {

Recycler::Recycler() = default;

std::vector<std::string> Recycler::get_instance_ids() {
    std::vector<std::string> instance_ids;
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
        return instance_ids;
    }

    std::unique_ptr<RangeGetIterator> it;
    do {
        ret = txn->get(key0, key1, &it);
        if (ret != 0) {
            LOG(WARNING) << "internal error, failed to get instance, ret=" << ret;
            return instance_ids;
        }

        while (it->has_next()) {
            auto [k, v] = it->next();
            if (!it->has_next()) key0 = k;

            LOG(INFO) << "range get instance_key=" << hex(k);
            // 0x01 "instance" ${instance_id} -> InstanceInfoPB
            k.remove_prefix(1); // Remove key space
            std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
            ret = decode_key(&k, &out);
            if (ret != 0) {
                LOG(WARNING) << "failed to decode key, ret=" << ret;
                continue;
            }
            if (out.size() != 2) {
                LOG(WARNING) << "decoded size no match, expect 2, given=" << out.size();
                continue;
            }
            auto instance_id = std::get<std::string>(std::get<0>(out[1]));

            LOG(INFO) << "get an instance, instance_id=" << instance_id;
            instance_ids.push_back(std::move(instance_id));
        }
        key0.push_back('\x00'); // Update to next smallest key for iteration
    } while (it->more());

    return instance_ids;
}

int Recycler::start() {
    int ret = 0;
    txn_kv_ = std::make_shared<FdbTxnKv>();
    ret = txn_kv_->init();
    if (ret != 0) {
        LOG(WARNING) << "failed to init txnkv, ret=" << ret;
        return 1;
    }

    S3Conf s3_conf;
    s3_conf.ak = config::test_s3_ak;
    s3_conf.sk = config::test_s3_sk;
    s3_conf.endpoint = config::test_s3_endpoint;
    s3_conf.region = config::test_s3_region;
    auto accessor = std::make_shared<S3Accessor>(std::move(s3_conf));
    ret = accessor->init();
    if (ret != 0) {
        LOG(WARNING) << "failed to init s3 accessor, ret=" << ret;
        return 1;
    }

    impl_ = std::make_unique<RecyclerImpl>(txn_kv_, accessor);

    do {
        auto instance_ids = get_instance_ids();
        LOG(INFO) << "get " << instance_ids.size() << " instance_id";

        auto start_time = std::chrono::steady_clock::now();

        // TODO(cyx): need a effient scheduling model
        std::vector<std::thread> threads;
        for (const auto& instance_id : instance_ids) {
            threads.push_back(std::thread([=]() {
                impl_->recycle_indexes(instance_id);
                impl_->recycle_partitions(instance_id);
                impl_->recycle_tmp_rowsets(instance_id);
                impl_->recycle_rowsets(instance_id);
            }));
        }
        for (auto& t : threads) {
            t.join();
        }

        std::this_thread::sleep_until(start_time +
                                      std::chrono::seconds(config::recycl_interval_seconds));
    } while (true); // TODO(cyx): graceful exit

    return 0;
}

RecyclerImpl::RecyclerImpl(std::shared_ptr<TxnKv> txn_kv, std::shared_ptr<S3Accessor> accessor)
        : txn_kv_(std::move(txn_kv)), accessor_(std::move(accessor)) {}

void RecyclerImpl::recycle_indexes(const std::string& instance_id) {
    int num_scanned = 0;
    int num_expired = 0;
    int num_recycled = 0;

    RecycleIndexKeyInfo index_key_info0 {instance_id, 0};
    RecycleIndexKeyInfo index_key_info1 {instance_id, std::numeric_limits<int64_t>::max()};
    std::string index_key0;
    std::string index_key1;
    recycle_index_key(index_key_info0, &index_key0);
    recycle_index_key(index_key_info1, &index_key1);

    LOG_INFO("begin to recycle indexes").tag("instance_id", instance_id);

    using namespace std::chrono;
    auto start_time = steady_clock::now();

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG_INFO("recycle indexes finished, cost={}s", cost)
                .tag("instance_id", instance_id)
                .tag("num_scanned", num_scanned)
                .tag("num_expired", num_expired)
                .tag("num_recycled", num_recycled);
    });

    int64_t current_time = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();

    auto recycle_func = [&instance_id, &num_scanned, &num_expired, &num_recycled, current_time,
                         this](std::string_view k, std::string_view v) -> bool {
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
        if (recycle_tablets(instance_id, index_pb.table_id(), index_id) != 0) {
            LOG_WARNING("failed to recycle tablets under index")
                    .tag("table_id", index_pb.table_id())
                    .tag("instance_id", instance_id)
                    .tag("index_id", index_id);
            return false;
        }
        ++num_recycled;
        return true;
    };

    scan_and_recycle(index_key0, index_key1, std::move(recycle_func));
}

void RecyclerImpl::recycle_partitions(const std::string& instance_id) {
    int num_scanned = 0;
    int num_expired = 0;
    int num_recycled = 0;

    RecyclePartKeyInfo part_key_info0 {instance_id, 0};
    RecyclePartKeyInfo part_key_info1 {instance_id, std::numeric_limits<int64_t>::max()};
    std::string part_key0;
    std::string part_key1;
    recycle_partition_key(part_key_info0, &part_key0);
    recycle_partition_key(part_key_info1, &part_key1);

    LOG_INFO("begin to recycle partitions").tag("instance_id", instance_id);

    using namespace std::chrono;
    auto start_time = steady_clock::now();

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG_INFO("recycle partitions finished, cost={}s", cost)
                .tag("instance_id", instance_id)
                .tag("num_scanned", num_scanned)
                .tag("num_expired", num_expired)
                .tag("num_recycled", num_recycled);
    });

    int64_t current_time = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();

    auto recycle_func = [&instance_id, &num_scanned, &num_expired, &num_recycled, current_time,
                         this](std::string_view k, std::string_view v) -> bool {
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
            if (recycle_tablets(instance_id, part_pb.table_id(), index_id, partition_id) != 0) {
                LOG_WARNING("failed to recycle tablets under partition")
                        .tag("table_id", part_pb.table_id())
                        .tag("instance_id", instance_id)
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

int RecyclerImpl::recycle_tablets(const std::string& instance_id, int64_t table_id,
                                  int64_t index_id, int64_t partition_id) {
    int num_scanned = 0;
    int num_recycled = 0;

    std::string tablet_key0;
    std::string tablet_key1;
    if (partition_id > 0) {
        // recycle tablets in a partition belonging to the index
        MetaTabletKeyInfo tablet_key_info0 {instance_id, table_id, index_id, partition_id, 0};
        MetaTabletKeyInfo tablet_key_info1 {instance_id, table_id, index_id, partition_id + 1, 0};
        meta_tablet_key(tablet_key_info0, &tablet_key0);
        meta_tablet_key(tablet_key_info1, &tablet_key1);
    } else {
        // recycle tablets in the index
        MetaTabletKeyInfo tablet_key_info0 {instance_id, table_id, index_id, 0, 0};
        MetaTabletKeyInfo tablet_key_info1 {instance_id, table_id, index_id + 1, 0, 0};
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
                .tag("instance_id", instance_id)
                .tag("table_id", table_id)
                .tag("index_id", index_id)
                .tag("num_scanned", num_scanned)
                .tag("num_recycled", num_recycled);
    });

    auto recycle_func = [&instance_id, &num_scanned, &num_recycled, this](
                                std::string_view k, std::string_view v) -> bool {
        ++num_scanned;
        doris::TabletMetaPB tablet_meta_pb;
        if (!tablet_meta_pb.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed tablet meta").tag("key", hex(k)).tag("val", hex(v));
            return false;
        }
        if (recycle_tablet(instance_id, tablet_meta_pb.tablet_id()) != 0) {
            LOG_WARNING("failed to recycle tablet")
                    .tag("instance_id", instance_id)
                    .tag("tablet_id", tablet_meta_pb.tablet_id());
            return false;
        }
        ++num_recycled;
        return true;
    };

    return scan_and_recycle(tablet_key0, tablet_key1, std::move(recycle_func), true);
}

int RecyclerImpl::recycle_tablet(const std::string& instance_id, int64_t tablet_id) {
    int num_scanned = 0;
    int num_recycled = 0;

    LOG_INFO("begin to recycle rowsets in a dropped tablet")
            .tag("instance_id", instance_id)
            .tag("tablet_id", tablet_id);

    using namespace std::chrono;
    auto start_time = steady_clock::now();

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG_INFO("recycle rowsets finished, cost={}s", cost)
                .tag("instance_id", instance_id)
                .tag("tablet_id", tablet_id)
                .tag("num_scanned", num_scanned)
                .tag("num_recycled", num_recycled);
    });

    // recycle committed rowsets in the tablet
    MetaRowsetKeyInfo rs_key_info0 {instance_id, tablet_id, 0};
    MetaRowsetKeyInfo rs_key_info1 {instance_id, tablet_id + 1, 0};
    std::string rs_key0;
    std::string rs_key1;
    meta_rowset_key(rs_key_info0, &rs_key0);
    meta_rowset_key(rs_key_info1, &rs_key1);

    auto recycle_committed_rowset = [&instance_id, &num_scanned, &num_recycled, this](
                                            std::string_view k, std::string_view v) -> bool {
        ++num_scanned;
        doris::RowsetMetaPB rs_meta_pb;
        if (!rs_meta_pb.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed rowset meta").tag("key", hex(k)).tag("val", hex(v));
            return false;
        }
        if (!rs_meta_pb.has_s3_bucket() || !rs_meta_pb.has_s3_prefix()) {
            LOG_WARNING("rowset meta has empty s3_bucket or s3_prefix info")
                    .tag("instance_id", instance_id)
                    .tag("rowset_id", rs_meta_pb.rowset_id_v2());
            ++num_recycled;
            return true;
        }
        const auto& rowset_id = rs_meta_pb.rowset_id_v2();
        int64_t num_segments = rs_meta_pb.num_segments();
        std::vector<std::string> segment_paths;
        segment_paths.reserve(num_segments);
        for (int64_t i = 0; i < num_segments; ++i) {
            segment_paths.push_back(
                    fmt::format("{}/{}_{}.dat", rs_meta_pb.s3_prefix(), rowset_id, i));
        }
        LOG_INFO("begin to delete rowset")
                .tag("bucket", rs_meta_pb.s3_bucket())
                .tag("prefix", rs_meta_pb.s3_prefix())
                .tag("rowset_id", rowset_id)
                .tag("num_segments", num_segments);
        int ret = accessor_->delete_objects(rs_meta_pb.s3_bucket(), segment_paths);
        if (ret != 0) {
            LOG_WARNING("failed to delete rowset")
                    .tag("bucket", rs_meta_pb.s3_bucket())
                    .tag("prefix", rs_meta_pb.s3_prefix())
                    .tag("rowset_id", rowset_id);
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
    RecycleRowsetKeyInfo recyc_rs_key_info0 {instance_id, tablet_id, ""};
    RecycleRowsetKeyInfo recyc_rs_key_info1 {instance_id, tablet_id + 1, ""};
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
        if (!recyc_rs_pb.has_obj_bucket() || !recyc_rs_pb.has_obj_prefix()) {
            // This rowset may be a delete predicate rowset without data.
            ++num_recycled;
            return true;
        }
        // decode rowset_id
        auto k1 = k;
        k1.remove_prefix(1);
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        decode_key(&k1, &out);
        // 0x01 "recycle" ${instance_id} "rowset" ${tablet_id} ${rowset_id} -> RecycleRowsetPB
        const auto& rowset_id = std::get<std::string>(std::get<0>(out[4]));
        LOG_INFO("begin to delete rowset")
                .tag("bucket", recyc_rs_pb.obj_bucket())
                .tag("prefix", recyc_rs_pb.obj_prefix())
                .tag("rowset_id", rowset_id);
        int ret = accessor_->delete_objects_by_prefix(recyc_rs_pb.obj_bucket(),
                                                      recyc_rs_pb.obj_prefix() + '/' + rowset_id);
        if (ret != 0) {
            LOG_WARNING("failed to delete rowset")
                    .tag("bucket", recyc_rs_pb.obj_bucket())
                    .tag("prefix", recyc_rs_pb.obj_prefix())
                    .tag("rowset_id", rowset_id);
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

void RecyclerImpl::recycle_rowsets(const std::string& instance_id) {
    int num_scanned = 0;
    int num_expired = 0;
    int num_recycled = 0;

    RecycleRowsetKeyInfo recyc_rs_key_info0 {instance_id, 0, ""};
    RecycleRowsetKeyInfo recyc_rs_key_info1 {instance_id, std::numeric_limits<int64_t>::max(), ""};
    std::string recyc_rs_key0;
    std::string recyc_rs_key1;
    recycle_rowset_key(recyc_rs_key_info0, &recyc_rs_key0);
    recycle_rowset_key(recyc_rs_key_info1, &recyc_rs_key1);

    LOG_INFO("begin to recycle rowsets").tag("instance_id", instance_id);

    using namespace std::chrono;
    auto start_time = steady_clock::now();

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG_INFO("recycle rowsets finished, cost={}s", cost)
                .tag("instance_id", instance_id)
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
        if (!recyc_rs_pb.has_obj_bucket() || !recyc_rs_pb.has_obj_prefix()) {
            // This rowset may be a delete predicate rowset without data.
            ++num_recycled;
            return true;
        }
        // decode rowset_id
        auto k1 = k;
        k1.remove_prefix(1);
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        decode_key(&k1, &out);
        // 0x01 "recycle" ${instance_id} "rowset" ${tablet_id} ${rowset_id} -> RecycleRowsetPB
        const auto& rowset_id = std::get<std::string>(std::get<0>(out[4]));
        LOG_INFO("begin to delete rowset")
                .tag("bucket", recyc_rs_pb.obj_bucket())
                .tag("prefix", recyc_rs_pb.obj_prefix())
                .tag("rowset_id", rowset_id);
        int ret = accessor_->delete_objects_by_prefix(recyc_rs_pb.obj_bucket(),
                                                      recyc_rs_pb.obj_prefix() + '/' + rowset_id);
        if (ret != 0) {
            LOG_WARNING("failed to delete rowset")
                    .tag("bucket", recyc_rs_pb.obj_bucket())
                    .tag("prefix", recyc_rs_pb.obj_prefix())
                    .tag("rowset_id", rowset_id);
            return false;
        }
        ++num_recycled;
        return true;
    };

    scan_and_recycle(recyc_rs_key0, recyc_rs_key1, std::move(recycle_func));
}

void RecyclerImpl::recycle_tmp_rowsets(const std::string& instance_id) {
    int num_scanned = 0;
    int num_expired = 0;
    int num_recycled = 0;

    MetaRowsetTmpKeyInfo tmp_rs_key_info0 {instance_id, 0, 0};
    MetaRowsetTmpKeyInfo tmp_rs_key_info1 {instance_id, std::numeric_limits<int64_t>::max(), 0};
    std::string tmp_rs_key0;
    std::string tmp_rs_key1;
    meta_rowset_tmp_key(tmp_rs_key_info0, &tmp_rs_key0);
    meta_rowset_tmp_key(tmp_rs_key_info1, &tmp_rs_key1);

    LOG_INFO("begin to recycle tmp rowsets").tag("instance_id", instance_id);

    using namespace std::chrono;
    auto start_time = steady_clock::now();

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG_INFO("recycle tmp rowsets finished, cost={}s", cost)
                .tag("instance_id", instance_id)
                .tag("num_scanned", num_scanned)
                .tag("num_expired", num_expired)
                .tag("num_recycled", num_recycled);
    });

    int64_t current_time = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();

    auto recycle_func = [&instance_id, &num_scanned, &num_expired, &num_recycled, current_time,
                         this](std::string_view k, std::string_view v) -> bool {
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
        if (!rs_meta_pb.has_s3_bucket() || !rs_meta_pb.has_s3_prefix()) {
            LOG_WARNING("rowset meta has empty s3_bucket or s3_prefix info")
                    .tag("instance_id", instance_id)
                    .tag("rowset_id", rs_meta_pb.rowset_id_v2());
            ++num_recycled;
            return true;
        }
        const auto& rowset_id = rs_meta_pb.rowset_id_v2();
        int64_t num_segments = rs_meta_pb.num_segments();
        std::vector<std::string> segment_paths;
        segment_paths.reserve(num_segments);
        for (int64_t i = 0; i < num_segments; ++i) {
            segment_paths.push_back(
                    fmt::format("{}/{}_{}.dat", rs_meta_pb.s3_prefix(), rowset_id, i));
        }
        LOG_INFO("begin to delete rowset")
                .tag("bucket", rs_meta_pb.s3_bucket())
                .tag("prefix", rs_meta_pb.s3_prefix())
                .tag("rowset_id", rowset_id)
                .tag("num_segments", num_segments);
        int ret = accessor_->delete_objects(rs_meta_pb.s3_bucket(), segment_paths);
        if (ret != 0) {
            LOG_WARNING("failed to delete rowset")
                    .tag("bucket", rs_meta_pb.s3_bucket())
                    .tag("prefix", rs_meta_pb.s3_prefix())
                    .tag("rowset_id", rowset_id);
            return false;
        }
        ++num_recycled;
        return true;
    };

    scan_and_recycle(tmp_rs_key0, tmp_rs_key1, std::move(recycle_func));
}

int RecyclerImpl::scan_and_recycle(
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
    } while (it->more());
    return ret;
}

} // namespace selectdb
