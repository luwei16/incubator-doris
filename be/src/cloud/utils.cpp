#include "cloud/utils.h"

#include "olap/storage_engine.h"

namespace doris::cloud {

DataDir* cloud_data_dir() {
    return StorageEngine::instance()->get_stores().front();
}

MetaMgr* meta_mgr() {
    return StorageEngine::instance()->meta_mgr();
}

} // namespace doris::cloud
