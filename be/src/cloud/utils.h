#pragma once

#include "cloud/meta_mgr.h"

namespace doris {

class DataDir;

namespace cloud {

// Get global cloud DataDir.
// We use `fs` under cloud DataDir as destination s3 storage for load tasks.
DataDir* cloud_data_dir();

// Get global MetaMgr.
MetaMgr* meta_mgr();

} // namespace cloud
} // namespace doris
