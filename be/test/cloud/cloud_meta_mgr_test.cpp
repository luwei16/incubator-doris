#include "cloud/cloud_meta_mgr.h"

#include <gtest/gtest.h>

#include <memory>

#include "olap/rowset/unique_rowset_id_generator.h"
#include "olap/tablet_meta.h"

namespace doris::cloud {

static CloudMetaMgr* meta_mgr = nullptr;
static UniqueRowsetIdGenerator* id_generator = nullptr;

class CloudMetaMgrTest : public testing::Test {
public:
    static void SetUpTestSuite() {
        meta_mgr = new CloudMetaMgr();
        id_generator = new UniqueRowsetIdGenerator({1, 2});
        ASSERT_EQ(Status::OK(), meta_mgr->open());
    }

    static void TearDownTestSuite() {
        delete meta_mgr;
        delete id_generator;
    }
};

static RowsetMetaSharedPtr create_rowset_meta(int64_t tablet_id, Version version, int64_t txn_id) {
    auto rs_meta = std::make_shared<RowsetMeta>();
    rs_meta->set_tablet_id(tablet_id);
    rs_meta->set_rowset_id(id_generator->next_id());
    rs_meta->set_start_version(version.first);
    rs_meta->set_end_version(version.second);
    rs_meta->set_txn_id(txn_id);
    return rs_meta;
}

static TabletMetaSharedPtr create_tablet_meta(int64_t table_id, int64_t tablet_id) {
    auto tablet_meta = std::make_shared<TabletMeta>();
    tablet_meta->_table_id = table_id;
    tablet_meta->_tablet_id = tablet_id;
    return tablet_meta;
}

TEST_F(CloudMetaMgrTest, write_rowset_meta) {
    int64_t ts = time(nullptr);
    {
        auto rs_meta1 = create_rowset_meta(10005, {0, 0}, ts);
        auto rs_meta2 = create_rowset_meta(10005, {0, 0}, ts);
        ASSERT_EQ(Status::OK(), meta_mgr->prepare_rowset(rs_meta1, true));
        ASSERT_EQ(Status::OK(), meta_mgr->commit_rowset(rs_meta1, true));
        ASSERT_TRUE(meta_mgr->prepare_rowset(rs_meta2, true).is_already_exist());
    }
    {
        auto rs_meta1 = create_rowset_meta(10006, {0, 0}, ts + 1);
        auto rs_meta2 = create_rowset_meta(10006, {0, 0}, ts + 1);
        ASSERT_EQ(Status::OK(), meta_mgr->prepare_rowset(rs_meta1, true));
        ASSERT_EQ(Status::OK(), meta_mgr->prepare_rowset(rs_meta2, true));
        ASSERT_EQ(Status::OK(), meta_mgr->commit_rowset(rs_meta1, true));
        ASSERT_TRUE(meta_mgr->commit_rowset(rs_meta2, true).is_already_exist());
    }
    {
        auto rs_meta1 = create_rowset_meta(10007, {0, 0}, ts + 2);
        auto rs_meta2 = create_rowset_meta(10007, {0, 0}, ts + 2);
        ASSERT_EQ(Status::OK(), meta_mgr->prepare_rowset(rs_meta1, true));
        ASSERT_EQ(Status::OK(), meta_mgr->prepare_rowset(rs_meta2, true));
        ASSERT_EQ(Status::OK(), meta_mgr->commit_rowset(rs_meta2, true));
        ASSERT_TRUE(meta_mgr->commit_rowset(rs_meta1, true).is_already_exist());
    }
}

TEST_F(CloudMetaMgrTest, write_and_get_rowset_meta) {
    int64_t ts = time(nullptr);
    auto rs_meta1 = create_rowset_meta(ts, {0, 1}, 0);
    ASSERT_EQ(Status::OK(), meta_mgr->prepare_rowset(rs_meta1, false));
    ASSERT_EQ(Status::OK(), meta_mgr->commit_rowset(rs_meta1, false));
    auto rs_meta2 = create_rowset_meta(ts, {2, 2}, 0);
    ASSERT_EQ(Status::OK(), meta_mgr->prepare_rowset(rs_meta2, false));
    ASSERT_EQ(Status::OK(), meta_mgr->commit_rowset(rs_meta2, false));
    auto rs_meta3 = create_rowset_meta(ts, {3, 3}, 0);
    ASSERT_EQ(Status::OK(), meta_mgr->prepare_rowset(rs_meta3, false));
    ASSERT_EQ(Status::OK(), meta_mgr->commit_rowset(rs_meta3, false));
    auto rs_meta4 = create_rowset_meta(ts, {4, 4}, 0);
    ASSERT_EQ(Status::OK(), meta_mgr->prepare_rowset(rs_meta4, false));
    ASSERT_EQ(Status::OK(), meta_mgr->commit_rowset(rs_meta4, false));
    std::vector<RowsetMetaSharedPtr> rs_metas;
    ASSERT_EQ(Status::OK(), meta_mgr->get_rowset_meta(ts, {0, 4}, &rs_metas));
    ASSERT_EQ(4, rs_metas.size());
    ASSERT_EQ(*rs_metas[0], *rs_meta1);
    ASSERT_EQ(*rs_metas[1], *rs_meta2);
    ASSERT_EQ(*rs_metas[2], *rs_meta3);
    ASSERT_EQ(*rs_metas[3], *rs_meta4);
    ASSERT_EQ(Status::OK(), meta_mgr->get_rowset_meta(ts, {0, 2}, &rs_metas));
    ASSERT_EQ(2, rs_metas.size());
    ASSERT_EQ(*rs_metas[0], *rs_meta1);
    ASSERT_EQ(*rs_metas[1], *rs_meta2);
}

TEST_F(CloudMetaMgrTest, write_and_get_tablet_meta) {
    auto tablet_meta = create_tablet_meta(10125, 10025);
    ASSERT_EQ(Status::OK(), meta_mgr->write_tablet_meta(tablet_meta));
    TabletMetaSharedPtr tablet_meta1;
    ASSERT_EQ(Status::OK(), meta_mgr->get_tablet_meta(10025, &tablet_meta1));
    ASSERT_EQ(*tablet_meta, *tablet_meta1);
}

} // namespace doris::cloud
