#include "cloud/cloud_meta_mgr.h"

#include <gtest/gtest.h>

#include <memory>

#include "olap/rowset/unique_rowset_id_generator.h"

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

} // namespace doris::cloud
