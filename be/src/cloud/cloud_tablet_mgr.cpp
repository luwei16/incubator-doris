#include "cloud/cloud_tablet_mgr.h"

#include <condition_variable>
#include <mutex>
#include <variant>

#include "cloud/utils.h"
#include "common/config.h"
#include "common/sync_point.h"

namespace doris::cloud {

// port from
// https://github.com/golang/groupcache/blob/master/singleflight/singleflight.go
template <typename Key, typename Val>
class SingleFlight {
public:
    SingleFlight() = default;

    SingleFlight(const SingleFlight&) = delete;
    void operator=(const SingleFlight&) = delete;

    using Loader = std::function<std::shared_ptr<Val>(const Key&)>;

    // Do executes and returns the results of the given function, making
    // sure that only one execution is in-flight for a given key at a
    // time. If a duplicate comes in, the duplicate caller waits for the
    // original to complete and receives the same results.
    std::shared_ptr<Val> Do(const Key& key, Loader loader) {
        std::unique_lock lock(_call_map_mtx);

        auto it = _call_map.find(key);
        if (it != _call_map.end()) {
            auto call = it->second;
            lock.unlock();

            std::unique_lock call_lock(call->mtx);
            call->condv.wait(call_lock, [&call]() { return call->done; });
            return call->val;
        }
        auto call = std::make_shared<Call>();
        _call_map.emplace(key, call);
        lock.unlock();

        call->val = loader(key);
        {
            std::lock_guard call_lock(call->mtx);
            call->done = true;
            call->condv.notify_all();
        }

        lock.lock();
        _call_map.erase(key);
        lock.unlock();

        return call->val;
    }

private:
    // `Call` is an in-flight or completed `Do` call
    struct Call {
        std::mutex mtx;
        std::condition_variable condv;
        bool done = false;
        std::shared_ptr<Val> val;
    };

    std::mutex _call_map_mtx;
    std::unordered_map<Key, std::shared_ptr<Call>> _call_map;
};

static SingleFlight<int64_t, std::variant<Status, TabletSharedPtr>> s_singleflight_load_tablet;

// TODO(cyx): multi shard to increase concurrency
static std::mutex s_tablet_map_mtx;
// tablet_id -> cached tablet
// This map owns all cached tablets. The lifetime of tablet can be longer than the LRU handle which holds its ref.
// It's also used for scenarios where users want to access the tablet by `tablet_id` without changing the LRU order.
static std::unordered_map<int64_t, TabletSharedPtr> s_tablet_map;

CloudTabletMgr::CloudTabletMgr()
        : _cache(new_lru_cache("TabletCache", config::tablet_cache_capacity, LRUCacheType::NUMBER,
                               config::tablet_cache_shards)) {}

CloudTabletMgr::~CloudTabletMgr() = default;

Status CloudTabletMgr::get_tablet(int64_t tablet_id, TabletSharedPtr* tablet) {
    // LRU value type, holds the ref of cached tablet
    struct Value {
        Tablet* tablet;
        int64_t tablet_id;
    };

    auto tablet_id_str = std::to_string(tablet_id);
    CacheKey key(tablet_id_str);
    auto handle = _cache->lookup(key);
    TEST_SYNC_POINT_CALLBACK("CloudTabletMgr::get_tablet", handle);

    if (handle == nullptr) {
        auto load_tablet = [this, &key](int64_t tablet_id)
                -> std::shared_ptr<std::variant<Status, TabletSharedPtr>> {
            auto res = std::make_shared<std::variant<Status, TabletSharedPtr>>();

            TabletMetaSharedPtr tablet_meta;
            auto st = meta_mgr()->get_tablet_meta(tablet_id, &tablet_meta);
            if (!st.ok()) {
                *res = std::move(st);
                return res;
            }
            auto tablet = std::make_shared<Tablet>(std::move(tablet_meta), cloud::cloud_data_dir());
            auto value = new Value();
            value->tablet = tablet.get();
            value->tablet_id = tablet->tablet_id(); 
            st = meta_mgr()->sync_tablet_rowsets(tablet.get());
            // ignore failure here because we will sync this tablet before query
            if (!st.ok()) {
                LOG_WARNING("sync tablet {} failed", tablet_id).error(st);
            }
            static auto deleter = [](const CacheKey& key, void* value) {
                auto value1 = reinterpret_cast<Value*>(value);
                {
                    // tablet has been evicted, release it from `s_tablet_map`
                    std::lock_guard lock(s_tablet_map_mtx);
                    // ref tablet may be released by another tablet with same `tablet_id` here, MUST NOT access memory in ref tablet
                    auto it = s_tablet_map.find(value1->tablet_id);
                    if (it != s_tablet_map.end() && it->second.get() == value1->tablet) {
                        s_tablet_map.erase(it);
                    }
                }
                delete value1;
            };

            auto handle = _cache->insert(key, value, 1, deleter);
            {
                std::lock_guard lock(s_tablet_map_mtx);
                s_tablet_map[tablet_id] = std::move(tablet);
            }
            *res = std::shared_ptr<Tablet>(value->tablet,
                                           [this, handle](...) { _cache->release(handle); });
            return res;
        };

        auto res = s_singleflight_load_tablet.Do(tablet_id, std::move(load_tablet));
        if (auto st = std::get_if<Status>(res.get())) {
            return *st;
        }
        *tablet = std::get<TabletSharedPtr>(*res);
        return Status::OK();
    }

    Tablet* tablet1 = reinterpret_cast<Value*>(_cache->value(handle))->tablet;
    *tablet = std::shared_ptr<Tablet>(tablet1, [this, handle](...) { _cache->release(handle); });
    return Status::OK();
}

void CloudTabletMgr::erase_tablet(int64_t tablet_id) {
    auto tablet_id_str = std::to_string(tablet_id);
    CacheKey key(tablet_id_str.data(), tablet_id_str.size());
    _cache->erase(key);
}

void CloudTabletMgr::vacuum_stale_rowsets() {
    std::vector<int64_t> tablets_to_vacuum;
    {
        std::lock_guard lock(_vacuum_set_mtx);
        tablets_to_vacuum = std::vector<int64_t>(_vacuum_set.begin(), _vacuum_set.end());
    }
    int num_vacuumed = 0;
    for (int64_t tablet_id : tablets_to_vacuum) {
        TabletSharedPtr tablet;
        {
            std::lock_guard lock(s_tablet_map_mtx);
            auto it = s_tablet_map.find(tablet_id);
            if (it == s_tablet_map.end()) {
                continue;
            }
            tablet = it->second;
        }
        num_vacuumed += tablet->cloud_delete_expired_stale_rowsets();
        {
            std::shared_lock tablet_rlock(tablet->get_header_lock());
            if (!tablet->has_stale_rowsets()) {
                std::lock_guard lock(_vacuum_set_mtx);
                _vacuum_set.erase(tablet_id);
            }
        }
    }
    LOG_INFO("finish vacuum stale rowsets").tag("num_vacuumed", num_vacuumed);
}

void CloudTabletMgr::add_to_vacuum_set(int64_t tablet_id) {
    std::lock_guard lock(_vacuum_set_mtx);
    _vacuum_set.insert(tablet_id);
}

std::vector<std::weak_ptr<Tablet>> CloudTabletMgr::get_weak_tablets() {
    std::vector<std::weak_ptr<Tablet>> weak_tablets;
    std::lock_guard lock(s_tablet_map_mtx);
    weak_tablets.reserve(s_tablet_map.size());
    for (auto& [_, tablet] : s_tablet_map) {
        weak_tablets.push_back(tablet);
    }
    return weak_tablets;
}

void CloudTabletMgr::sync_tablets() {
    using namespace std::chrono;
    int64_t last_sync_time_bound =
            duration_cast<seconds>(system_clock::now().time_since_epoch()).count() -
            config::tablet_sync_interval_seconds;

    auto weak_tablets = get_weak_tablets();

    // sort by last_sync_time
    static auto cmp = [](const auto& a, const auto& b) { return a.first < b.first; };
    std::multiset<std::pair<int64_t, std::weak_ptr<Tablet>>, decltype(cmp)> sync_time_tablet_set(
            cmp);

    for (auto& weak_tablet : weak_tablets) {
        if (auto tablet = weak_tablet.lock()) {
            int64_t last_sync_time = tablet->last_sync_time();
            if (last_sync_time <= last_sync_time_bound) {
                sync_time_tablet_set.emplace(last_sync_time, weak_tablet);
            }
        }
    }

    for (auto& [_, weak_tablet] : sync_time_tablet_set) {
        if (auto tablet = weak_tablet.lock()) {
            if (tablet->last_sync_time() > last_sync_time_bound) {
                continue;
            }
            auto st = tablet->cloud_sync_rowsets();
            if (!st.ok()) {
                LOG_WARNING("failed to sync tablet {}", tablet->tablet_id()).error(st);
            }
        }
    }
}

} // namespace doris::cloud
