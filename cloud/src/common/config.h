// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "configbase.h"

namespace selectdb::config {

CONF_String(meta_service_conf_path, "./conf/meta_service.conf");
CONF_Int32(brpc_listen_port, "5000");
CONF_Int32(brpc_num_threads, "-1");
CONF_String(fdb_cluster_file_path, "./conf/fdb.cluster");
CONF_String(http_token, "greedisgood9999");
// use volatile mem kv for test. MUST NOT be `true` in production environment.
CONF_Bool(use_mem_kv, "false");

// logging
CONF_String(log_dir, "./log/");
CONF_String(log_level, "info"); // info warn error fatal
CONF_Int64(log_size_mb, "1024");
CONF_Int32(log_filenum_quota, "10");
CONF_Bool(log_immediate_flush, "false");
CONF_Strings(log_verbose_modules, ""); // Comma seprated list: a.*,b.*
CONF_Int32(log_verbose_level, "5");

// recycler config
CONF_mInt64(recycl_interval_seconds, "3600");
CONF_mInt64(index_retention_seconds, "172800");    // 48h
CONF_mInt64(partition_retention_seconds, "86400"); // 24h
CONF_mInt64(rowset_retention_seconds, "10800");    // 3h
CONF_Int32(recycle_concurrency, "16");
CONF_Bool(recycle_standalone_mode, "false");

CONF_String(test_s3_ak, "ak");
CONF_String(test_s3_sk, "sk");
CONF_String(test_s3_endpoint, "endpoint");
CONF_String(test_s3_region, "region");
CONF_String(test_s3_bucket, "bucket");
// CONF_Int64(a, "1073741824");
// CONF_Bool(b, "true");

// txn config
CONF_Int32(stream_load_default_timeout_second, "600");
CONF_Int32(stream_load_default_precommit_timeout_second, "3600");
CONF_Int32(label_keep_max_second, "259200"); //3 * 24 * 3600 seconds
CONF_Int32(expired_txn_scan_key_nums, "1000");

} // namespace selectdb::config
