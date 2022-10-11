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

import com.selectdb.cloud.proto.SelectdbCloud.ObjectStoreInfoPB;
import com.selectdb.cloud.proto.SelectdbCloud.ObjectStoreInfoPB.Provider;
import com.selectdb.cloud.storage.RemoteBase.ObjectInfo;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class RemoteBaseTest {
    private String endpoint = "oss-cn-hongkong.aliyuncs.com";
    private String region = "oss-cn-hongkong";
    private String bucket = "cooldown-dev";
    private String prefix = "test_cluster";
    private String ak = "";
    private String sk = "";
    private ObjectStoreInfoPB.Provider provider = Provider.OSS;

    private ObjectStoreInfoPB objectStoreInfoPB = ObjectStoreInfoPB.newBuilder().setAk(ak).setSk(sk)
            .setProvider(provider).setBucket(bucket).setEndpoint(endpoint).setRegion(region).setPrefix(prefix).build();
    private ObjectInfo objectInfo = new ObjectInfo(objectStoreInfoPB);

    @Test
    public void list() throws Exception {
        RemoteBase remote = RemoteBase.newInstance(objectInfo);
        String token = null;
        try {
            int id = 0;
            while (true) {
                ListObjectsResult listObjectsResult = remote.listObjects(token);
                for (ObjectFile objectFile : listObjectsResult.getObjectInfoList()) {
                    System.out.println(
                            String.format("id: %d, key: %s, relativePath: %s, etag: %s", id++, objectFile.getKey(),
                                    objectFile.getRelativePath(), objectFile.getEtag()));
                }
                if (!listObjectsResult.isTruncated()) {
                    break;
                }
                token = listObjectsResult.getContinuationToken();
                if (id > 2500) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("should be success");
        } finally {
            remote.close();
        }
    }
}
