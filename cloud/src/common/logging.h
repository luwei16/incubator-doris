
#pragma once

#include <glog/logging.h>

namespace selectdb {

bool init_glog(const char* basename);

} // namespace selectdb

// To keep it simple and practical, we don't actually need so many VLOG levels.
// Using `VLOG(${number})` is confusing and hard to desid in most cases, all we
// need is a complementary debug level to glog's default 4 levels of logging.
// "One VLOG level to rule them all!"
#define DEBUG 5
// VLOG_DEBUG is alias of VLOG(DEBUG) I.O.W VLOG(5)
#define VLOG_DEBUG VLOG(DEBUG)
