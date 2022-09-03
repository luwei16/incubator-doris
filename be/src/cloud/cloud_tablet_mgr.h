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

private:
    std::unique_ptr<Cache> _cache;
};

} // namespace doris::cloud
