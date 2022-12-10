#pragma once

#include "cloud/cloud_tablet_mgr.h"
#include "cloud/meta_mgr.h"

namespace doris {

class DataDir;

namespace cloud {

// Get global cloud DataDir.
// We use `fs` under cloud DataDir as destination s3 storage for load tasks.
DataDir* cloud_data_dir();

// Get global MetaMgr.
MetaMgr* meta_mgr();

// Get global CloudTabletMgr
CloudTabletMgr* tablet_mgr();

// Get fs with latest object store info.
io::FileSystemSPtr latest_fs();

} // namespace cloud
} // namespace doris
