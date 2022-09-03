#include "cloud/utils.h"

#include "common/sync_point.h"
#include "olap/storage_engine.h"

namespace doris::cloud {

DataDir* cloud_data_dir() {
#ifdef BE_TEST
    return nullptr;
#endif
    return StorageEngine::instance()->get_stores().front();
}

MetaMgr* meta_mgr() {
#ifdef BE_TEST
    MetaMgr* ret;
#endif
    TEST_SYNC_POINT_RETURN_WITH_VALUE("meta_mgr", &ret);
    return StorageEngine::instance()->meta_mgr();
}

CloudTabletMgr* tablet_mgr() {
    static CloudTabletMgr tablet_mgr;
    return &tablet_mgr;
}

} // namespace doris::cloud
