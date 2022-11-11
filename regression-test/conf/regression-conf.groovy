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

/* ******* Do not commit this file unless you know what you are doing ******* */

isSmokeTest = true

smokeEnv = "dx-smoke-test"
defaultDb = "smoke_test"

jdbcUrl = "jdbc:mysql://127.0.0.1:8877/?"
jdbcUser = "admin"
jdbcPassword = ""

feCloudHttpAddress = "127.0.0.1:8876"
feCloudHttpUser = "admin"
feCloudHttpPassword = ""

// for cloud mode
instanceId = "clickbench"
cloudUniqueId = "cloud_unique_id_xxx"
clusterName = "cloud_cluster_for_smoke"

// set DORIS_HOME by system properties
// e.g. java -DDORIS_HOME=./
DORIS_HOME = "{DORIS_HOME}"
suitePath = "${DORIS_HOME}/regression-test/suites/cloud/smoke"
dataPath = "${DORIS_HOME}/regression-test/data/cloud/smoke"
pluginPath = "${DORIS_HOME}/regression-test/plugins"
realDataPath = "${DORIS_HOME}/regression-test/realdata"
// sf1DataPath can be url like "https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com" or local path like "/data"
// sf1DataPath = "https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com"

// cloud smoke test

// for external stage
ak="ak"
sk="sk"
objPrefix="dx-smoke-test"
s3Endpoint="cos.ap-beijing.myqcloud.com"
s3Region="ap-beijing"
s3BucketName="justtmp-bj-1308700295"
// must same as meta service's obj prefix, smoke test internal、external stage use the same prefix
s3Prefix="dx-test"
