#include "cloud/cloud_tablet_mgr.h"

#include <gtest/gtest.h>

#include "cloud/meta_mgr.h"
#include "common/config.h"
#include "common/sync_point.h"

namespace doris::cloud {

class MockMetaMgr final : public MetaMgr {
public:
    MockMetaMgr() = default;
    ~MockMetaMgr() override = default;

    Status get_tablet_meta(int64_t tablet_id, TabletMetaSharedPtr* tablet_meta) override {
        auto tablet_meta1 = std::make_shared<TabletMeta>();
        tablet_meta1->_tablet_id = tablet_id;
        *tablet_meta = std::move(tablet_meta1);
        return Status::OK();
    }

    Status get_rowset_meta(const TabletMetaSharedPtr& tablet_meta, Version version_range,
                           std::vector<RowsetMetaSharedPtr>* rs_metas) override {
        return Status::OK();
    }
};

TEST(CloudTabletMgrTest, normal) {
    auto sp = SyncPoint::get_instance();
    sp->clear_all_call_backs();
    sp->enable_processing();

    MockMetaMgr mock_meta_mgr;
    bool cache_missed;
    {
        sp->set_call_back("CloudTabletMgr::get_tablet",
                          [&cache_missed](void* handle) { cache_missed = (handle == nullptr); });

        sp->set_call_back("meta_mgr", [&mock_meta_mgr](void* ret) {
            *reinterpret_cast<MetaMgr**>(ret) = &mock_meta_mgr;
        });
        sp->set_call_back("meta_mgr::pred",
                          [](void* pred) { *reinterpret_cast<bool*>(pred) = true; });
    }

    config::tablet_cache_capacity = 1;
    config::tablet_cache_shards = 1;
    CloudTabletMgr tablet_mgr;

    Status st;
    TabletSharedPtr tablet;

    // test cache hit
    st = tablet_mgr.get_tablet(20000, &tablet);
    ASSERT_EQ(st, Status::OK());
    ASSERT_TRUE(cache_missed);

    st = tablet_mgr.get_tablet(20000, &tablet);
    ASSERT_EQ(st, Status::OK());
    ASSERT_FALSE(cache_missed);

    st = tablet_mgr.get_tablet(20001, &tablet); // evict tablet 20000
    ASSERT_EQ(st, Status::OK());
    ASSERT_TRUE(cache_missed);

    st = tablet_mgr.get_tablet(20000, &tablet);
    ASSERT_EQ(st, Status::OK());
    ASSERT_TRUE(cache_missed);

    // test cached value lifetime
    st = tablet_mgr.get_tablet(20000, &tablet);
    ASSERT_EQ(st, Status::OK());
    ASSERT_FALSE(cache_missed);

    tablet_mgr.erase_tablet(20000);
    ASSERT_EQ(20000, tablet->tablet_id());
}

} // namespace doris::cloud
