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

package org.apache.doris.load.loadv2;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.utframe.TestWithFeService;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Lists;
import com.selectdb.cloud.proto.SelectdbCloud;
import com.selectdb.cloud.proto.SelectdbCloud.ObjectFilePB;
import com.selectdb.cloud.proto.SelectdbCloud.ObjectStoreInfoPB.Provider;
import com.selectdb.cloud.storage.ListObjectsResult;
import com.selectdb.cloud.storage.ObjectFile;
import com.selectdb.cloud.storage.RemoteBase;
import com.selectdb.cloud.storage.RemoteBase.ObjectInfo;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class CopyLoadPendingTaskTest extends TestWithFeService {

    private ConnectContext ctx;
    private ObjectInfo objectInfo = new ObjectInfo(Provider.OSS, "test_ak", "test_sk", "test_bucket", "test_endpoint",
            "test_region", "test_prefix");
    @Mocked
    RemoteBase remote;

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testGlob() {
        // file name contains "," which is a special character in Glob
        String fileName = "1,csv";
        String globPrefix = "glob:";
        Assert.assertEquals(true, matchGlob(fileName, globPrefix + "1,csv"));
        Assert.assertEquals(false, matchGlob(fileName, globPrefix + "{1,csv}"));
        Assert.assertEquals(true, matchGlob(fileName, globPrefix + "{1\\,csv}"));
        Assert.assertEquals(true, matchGlob(fileName, globPrefix + "{1\\,csv,2\\,csv}"));
    }

    private boolean matchGlob(String file, String pattern) {
        PathMatcher pathMatcher = FileSystems.getDefault().getPathMatcher(pattern);
        Path path = Paths.get(file);
        return pathMatcher.matches(path);
    }

    @Test
    public void testParseFileForCopyJob() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        String prefix = "prefix1";
        List<String> subPrefixes = Lists.newArrayList("", "dir1", "dir2/dir3", "dir4/dir5/dir6");
        // list object files
        List<ObjectFile> objectFiles = new ArrayList<>();
        for (int i = 0; i < subPrefixes.size(); i++) {
            for (int j = 0; j < i + 1; j++) {
                String relativePath = subPrefixes.get(i) + (subPrefixes.get(i).isEmpty() ? "" : "/") + "file_" + j
                        + ".csv";
                String etag = "";
                ObjectFile objectFile = new ObjectFile(prefix + (prefix.isEmpty() ? "" : "/") + relativePath,
                        relativePath, etag, (j + 1) * 10);
                objectFiles.add(objectFile);
                System.out.println(
                        "object file=" + objectFile.getKey() + ", " + objectFile.getRelativePath() + ", size: "
                                + objectFile.getSize());
            }
        }
        ListObjectsResult listObjectsResult = new ListObjectsResult(objectFiles, false, null);
        // loading or loaded files
        List<SelectdbCloud.ObjectFilePB> files = new ArrayList<>();
        List<SelectdbCloud.ObjectFilePB> files2 = Lists.newArrayList(
                ObjectFilePB.newBuilder().setRelativePath("dir1/file_1.csv").setEtag("").build());
        new Expectations(ctx.getEnv(), ctx.getEnv().getInternalCatalog(), remote) {
            {
                Env.getCurrentInternalCatalog().getCopyFiles(anyString, 100);
                minTimes = 0;
                result = files;

                Env.getCurrentInternalCatalog().getCopyFiles(anyString, 200);
                minTimes = 0;
                result = files2;

                RemoteBase.newInstance(objectInfo);
                minTimes = 0;
                result = remote;

                remote.listObjects(anyString);
                minTimes = 0;
                result = listObjectsResult;
            }
        };

        String stageId = "1";
        long tableId = 100;
        long sizeLimit = 0;
        int fileNumLimit = 0;
        int fileMetaSizeLimit = 0;

        CopyLoadPendingTask task = new CopyLoadPendingTask(null, null, null);
        // test pattern
        List<Pair<String, Integer>> patternAndMatchNum = Lists.newArrayList(Pair.of(null, 10), Pair.of("file*csv", 1),
                Pair.of("**/file*csv", 9), Pair.of("file_0.csv", 1), Pair.of("*/file_[0-9].csv", 2));
        for (Pair<String, Integer> pair : patternAndMatchNum) {
            String pattern = pair.first;
            List<Pair<TBrokerFileStatus, ObjectFilePB>> fileStatus = new ArrayList<>();
            task.parseFileForCopyJob(stageId, tableId, pattern, sizeLimit, fileNumLimit, fileMetaSizeLimit, fileStatus,
                    objectInfo);
            Assert.assertTrue(pair.second == fileStatus.size());
        }
        // test loaded files is not empty
        do {
            String pattern = null;
            List<Pair<TBrokerFileStatus, ObjectFilePB>> fileStatus = new ArrayList<>();
            task.parseFileForCopyJob(stageId, 200, pattern, sizeLimit, fileNumLimit, fileMetaSizeLimit, fileStatus,
                    objectInfo);
            Assert.assertEquals(9, fileStatus.size());
        } while (false);
        // test size limit
        do {
            String pattern = null;
            List<Pair<TBrokerFileStatus, ObjectFilePB>> fileStatus = new ArrayList<>();
            task.parseFileForCopyJob(stageId, tableId, pattern, 100, fileNumLimit, fileMetaSizeLimit, fileStatus,
                    objectInfo);
            Assert.assertEquals(5, fileStatus.size());
        } while (false);
        // test file num limit
        do {
            String pattern = null;
            List<Pair<TBrokerFileStatus, ObjectFilePB>> fileStatus = new ArrayList<>();
            task.parseFileForCopyJob(stageId, tableId, pattern, sizeLimit, 6, fileMetaSizeLimit, fileStatus,
                    objectInfo);
            Assert.assertEquals(6, fileStatus.size());
        } while (false);
        // test file meta size limit
        do {
            String pattern = null;
            List<Pair<TBrokerFileStatus, ObjectFilePB>> fileStatus = new ArrayList<>();
            task.parseFileForCopyJob(stageId, tableId, pattern, sizeLimit, fileNumLimit, 60, fileStatus, objectInfo);
            Assert.assertEquals(3, fileStatus.size());
        } while (false);
        // test size and file num limit
        do {
            String pattern = null;
            List<Pair<TBrokerFileStatus, ObjectFilePB>> fileStatus = new ArrayList<>();
            task.parseFileForCopyJob(stageId, tableId, pattern, 100, fileNumLimit, fileMetaSizeLimit, fileStatus,
                    objectInfo);
            Assert.assertEquals(5, fileStatus.size());
        } while (false);
    }

    @Test
    public void testContinuationToken() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        String prefix = "prefix1";
        List<String> subPrefixes = Lists.newArrayList("", "dir1", "dir2/dir3", "dir4/dir5/dir6");
        // list object files
        List<ObjectFile> objectFiles = new ArrayList<>();
        for (int i = 0; i < subPrefixes.size(); i++) {
            for (int j = 0; j < i + 1; j++) {
                String relativePath = subPrefixes.get(i) + (subPrefixes.get(i).isEmpty() ? "" : "/") + "file_" + j
                        + ".csv";
                String etag = "";
                ObjectFile objectFile = new ObjectFile(prefix + (prefix.isEmpty() ? "" : "/") + relativePath,
                        relativePath, etag, (j + 1) * 10);
                objectFiles.add(objectFile);
                System.out.println(
                        "object file=" + objectFile.getKey() + ", " + objectFile.getRelativePath() + ", size: "
                                + objectFile.getSize());
            }
        }
        ListObjectsResult listObjectsResult = new ListObjectsResult(objectFiles, true, "abc");
        ListObjectsResult listObjectsResult2 = new ListObjectsResult(objectFiles, false, null);
        // loading or loaded files
        List<SelectdbCloud.ObjectFilePB> files = new ArrayList<>();
        new Expectations(ctx.getEnv(), ctx.getEnv().getInternalCatalog(), remote) {
            {
                Env.getCurrentInternalCatalog().getCopyFiles(anyString, 100);
                minTimes = 0;
                result = files;

                RemoteBase.newInstance(objectInfo);
                minTimes = 0;
                result = remote;

                remote.listObjects(null);
                minTimes = 0;
                result = listObjectsResult;

                remote.listObjects("abc");
                minTimes = 0;
                result = listObjectsResult2;
            }
        };

        String stageId = "1";
        long tableId = 100;
        long sizeLimit = 0;
        int fileNumLimit = 0;
        int fileMetaSizeLimit = 0;

        CopyLoadPendingTask task = new CopyLoadPendingTask(null, null, null);
        // test pattern
        List<Pair<String, Integer>> patternAndMatchNum = Lists.newArrayList(Pair.of(null, 10), Pair.of("file*csv", 1),
                Pair.of("**/file*csv", 9), Pair.of("file_0.csv", 1), Pair.of("*/file_[0-9].csv", 2));
        for (Pair<String, Integer> pair : patternAndMatchNum) {
            String pattern = pair.first;
            List<Pair<TBrokerFileStatus, ObjectFilePB>> fileStatus = new ArrayList<>();
            task.parseFileForCopyJob(stageId, tableId, pattern, sizeLimit, fileNumLimit, fileMetaSizeLimit, fileStatus,
                    objectInfo);
            Assert.assertTrue("expected: " + pair.second * 2 + ", real: " + fileStatus.size(),
                    pair.second * 2 == fileStatus.size());
        }
    }
}
