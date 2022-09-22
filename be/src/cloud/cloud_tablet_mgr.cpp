#include "cloud/cloud_tablet_mgr.h"

#include "cloud/utils.h"
#include "common/config.h"
#include "common/sync_point.h"
#include "olap/lru_cache.h"

namespace doris::cloud {

CloudTabletMgr::CloudTabletMgr()
        : _cache(new_lru_cache("TabletCache", config::tablet_cache_capacity, LRUCacheType::NUMBER,
                               config::tablet_cache_shards)) {}

CloudTabletMgr::~CloudTabletMgr() = default;

Status CloudTabletMgr::get_tablet(int64_t tablet_id, TabletSharedPtr* tablet) {
    auto tablet_id_str = std::to_string(tablet_id);
    CacheKey key(tablet_id_str);
    auto handle = _cache->lookup(key);
    TEST_SYNC_POINT_CALLBACK("CloudTabletMgr::get_tablet", handle);

    Tablet* tablet1 =
            handle == nullptr ? nullptr : reinterpret_cast<Tablet*>(_cache->value(handle));
    if (tablet1 == nullptr) {
        TabletMetaSharedPtr tablet_meta;
        RETURN_IF_ERROR(meta_mgr()->get_tablet_meta(tablet_id, &tablet_meta));
        std::vector<RowsetMetaSharedPtr> rs_metas;
        tablet1 = new Tablet(std::move(tablet_meta), cloud::cloud_data_dir());
        RETURN_IF_ERROR(meta_mgr()->sync_tablet_rowsets(tablet1));
        static auto deleter = [](const CacheKey& key, void* value) {
            delete (Tablet*)value; // Just delete to reclaim
        };
        handle = _cache->insert(key, tablet1, 1, deleter);
    }

    *tablet = std::shared_ptr<Tablet>(tablet1, [this, handle](...) { _cache->release(handle); });
    return Status::OK();
}

void CloudTabletMgr::erase_tablet(int64_t tablet_id) {
    auto tablet_id_str = std::to_string(tablet_id);
    CacheKey key(tablet_id_str.data(), tablet_id_str.size());
    _cache->erase(key);
}

} // namespace doris::cloud
