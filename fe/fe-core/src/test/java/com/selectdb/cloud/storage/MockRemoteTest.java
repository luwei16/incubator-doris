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

package com.selectdb.cloud.storage;

import com.selectdb.cloud.proto.SelectdbCloud.ObjectStoreInfoPB.Provider;
import com.selectdb.cloud.storage.RemoteBase.ObjectInfo;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class MockRemoteTest {
    private static final String STORAGE_PREFIX = "test_prefix";
    private ObjectInfo objectInfo = new ObjectInfo(Provider.OSS, "test_ak", "test_sk", "test_bucket", "test_endpoint",
            "test_region", STORAGE_PREFIX);

    @Test
    public void test() throws Exception {
        MockRemote mockRemote = new MockRemote(objectInfo);
        for (int i = 0; i < 10; i++) {
            mockRemote.addObject(STORAGE_PREFIX + "/file_" + i + ".csv");
            mockRemote.addObject(STORAGE_PREFIX + "/dir1/file_" + i + ".csv");
        }
        // list objects
        ListObjectsResult listObjectsResult = mockRemote.listObjects(null);
        Assert.assertEquals(20, listObjectsResult.getObjectInfoList().size());
        // list objects with prefix
        listObjectsResult = mockRemote.listObjects("", null);
        Assert.assertEquals(20, listObjectsResult.getObjectInfoList().size());
        listObjectsResult = mockRemote.listObjects("dir1", null);
        Assert.assertEquals(10, listObjectsResult.getObjectInfoList().size());
        // head object
        listObjectsResult = mockRemote.headObject("file_1.csv");
        Assert.assertEquals(1, listObjectsResult.getObjectInfoList().size());
        listObjectsResult = mockRemote.headObject("1.csv");
        Assert.assertEquals(0, listObjectsResult.getObjectInfoList().size());
    }
}
