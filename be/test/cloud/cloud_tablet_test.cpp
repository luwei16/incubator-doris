#include <gtest/gtest.h>

#include "olap/tablet.h"

namespace doris {

static RowsetMetaSharedPtr create_rowset_meta(Version version) {
    auto rs_meta = std::make_shared<RowsetMeta>();
    rs_meta->set_start_version(version.first);
    rs_meta->set_end_version(version.second);
    return rs_meta;
}

TEST(CloudTabletTest, calc_missed_versions) {
    {
        std::vector<RowsetMetaSharedPtr> rs_metas;
        rs_metas.push_back(create_rowset_meta({0, 4}));
        rs_metas.push_back(create_rowset_meta({5, 5}));
        rs_metas.push_back(create_rowset_meta({8, 8}));
        rs_metas.push_back(create_rowset_meta({9, 9}));
        rs_metas.push_back(create_rowset_meta({13, 13}));
        auto tablet_meta = std::make_shared<TabletMeta>();
        tablet_meta->_rs_metas = std::move(rs_metas);
        Tablet tablet(std::move(tablet_meta), nullptr);
        ASSERT_EQ(tablet.cloud_calc_missed_versions(3), Versions {});
        ASSERT_EQ(tablet.cloud_calc_missed_versions(4), Versions {});
        ASSERT_EQ(tablet.cloud_calc_missed_versions(5), Versions {});
        ASSERT_EQ(tablet.cloud_calc_missed_versions(6), (Versions {{6, 6}}));
        ASSERT_EQ(tablet.cloud_calc_missed_versions(7), (Versions {{6, 7}}));
        ASSERT_EQ(tablet.cloud_calc_missed_versions(8), (Versions {{6, 7}}));
        ASSERT_EQ(tablet.cloud_calc_missed_versions(9), (Versions {{6, 7}}));
        ASSERT_EQ(tablet.cloud_calc_missed_versions(10), (Versions {{6, 7}, {10, 10}}));
        ASSERT_EQ(tablet.cloud_calc_missed_versions(11), (Versions {{6, 7}, {10, 11}}));
        ASSERT_EQ(tablet.cloud_calc_missed_versions(12), (Versions {{6, 7}, {10, 12}}));
        ASSERT_EQ(tablet.cloud_calc_missed_versions(13), (Versions {{6, 7}, {10, 12}}));
        ASSERT_EQ(tablet.cloud_calc_missed_versions(14), (Versions {{6, 7}, {10, 12}, {14, 14}}));
    }
    {
        auto tablet_meta = std::make_shared<TabletMeta>();
        Tablet tablet(std::move(tablet_meta), nullptr);
        ASSERT_EQ(tablet.cloud_calc_missed_versions(6), (Versions {{0, 6}}));
    }
    {
        std::vector<RowsetMetaSharedPtr> rs_metas;
        rs_metas.push_back(create_rowset_meta({5, 5}));
        auto tablet_meta = std::make_shared<TabletMeta>();
        tablet_meta->_rs_metas = std::move(rs_metas);
        Tablet tablet(std::move(tablet_meta), nullptr);
        ASSERT_EQ(tablet.cloud_calc_missed_versions(4), (Versions {{0, 4}}));
        ASSERT_EQ(tablet.cloud_calc_missed_versions(6), (Versions {{0, 4}, {6, 6}}));
    }
}

} // namespace doris
