#pragma once

#include <memory>

#include "common/status.h"
#include "olap/lru_cache.h"
#include "olap/tablet.h"

namespace doris::cloud {

class CloudTabletMgr {
public:
    CloudTabletMgr();
    ~CloudTabletMgr();

    Status get_tablet(int64_t tablet_id, TabletSharedPtr* tablet);

    void erase_tablet(int64_t tablet_id);

    void vacuum_stale_rowsets();

    // MUST add tablet to vacuum set if it has stale rowsets.
    void add_to_vacuum_set(int64_t tablet_id);

private:
    std::unique_ptr<Cache> _cache;

    std::mutex _vacuum_set_mtx;
    std::unordered_set<int64_t> _vacuum_set;
};

} // namespace doris::cloud
