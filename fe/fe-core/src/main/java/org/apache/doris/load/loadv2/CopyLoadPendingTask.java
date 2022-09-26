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

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import org.apache.doris.thrift.TBrokerFileStatus;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.selectdb.cloud.proto.SelectdbCloud.ObjectFilePB;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

public class CopyLoadPendingTask extends BrokerLoadPendingTask {
    private static final Logger LOG = LogManager.getLogger(CopyLoadPendingTask.class);
    private Map<FileGroupAggKey, List<List<Pair<TBrokerFileStatus, ObjectFilePB>>>> fileStatusMap = Maps.newHashMap();

    public CopyLoadPendingTask(CopyJob loadTaskCallback,
            Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToBrokerFileGroups, BrokerDesc brokerDesc) {
        super(loadTaskCallback, aggKeyToBrokerFileGroups, brokerDesc);
    }

    @Override
    void executeTask() throws UserException {
        super.executeTask(); // get all files and begin txn
        beginCopy((BrokerPendingTaskAttachment) attachment);
    }

    @Override
    protected void getAllFileStatus() throws UserException {
        long start = System.currentTimeMillis();
        CopyJob copyJob = (CopyJob) callback;
        for (Map.Entry<FileGroupAggKey, List<BrokerFileGroup>> entry : aggKeyToBrokerFileGroups.entrySet()) {
            FileGroupAggKey aggKey = entry.getKey();
            List<BrokerFileGroup> fileGroups = entry.getValue();

            List<List<Pair<TBrokerFileStatus, ObjectFilePB>>> fileStatusList = Lists.newArrayList();
            long tableTotalFileSize = 0;
            int tableTotalFileNum = 0;
            int groupNum = 0;

            for (BrokerFileGroup fileGroup : fileGroups) {
                long groupFileSize = 0;
                List<Pair<TBrokerFileStatus, ObjectFilePB>> fileStatuses = Lists.newArrayList();
                for (String path : fileGroup.getFilePaths()) {
                    BrokerUtil.parseFileForCopyJob(copyJob.getStageId(), fileGroup.getTableId(), brokerDesc, path,
                            copyJob.getFilesOrPattern(), copyJob.getSizeLimit(), fileStatuses);
                }
                boolean isBinaryFileFormat = fileGroup.isBinaryFileFormat();
                List<Pair<TBrokerFileStatus, ObjectFilePB>> filteredFileStatuses = Lists.newArrayList();
                for (Pair<TBrokerFileStatus, ObjectFilePB> pair : fileStatuses) {
                    TBrokerFileStatus fstatus = pair.first;
                    if (fstatus.getSize() == 0 && isBinaryFileFormat) {
                        // For parquet or orc file, if it is an empty file, ignore it.
                        // Because we can not read an empty parquet or orc file.
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                    new LogBuilder(LogKey.LOAD_JOB, callback.getCallbackId()).add("empty file", fstatus)
                                            .build());
                        }
                    } else {
                        groupFileSize += fstatus.size;
                        filteredFileStatuses.add(pair);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(new LogBuilder(LogKey.LOAD_JOB, callback.getCallbackId()).add("file_status",
                                    fstatus).build());
                        }
                    }
                }
                fileStatusList.add(filteredFileStatuses);
                tableTotalFileSize += groupFileSize;
                tableTotalFileNum += filteredFileStatuses.size();
                LOG.info("get {} files in file group {} for table {}. size: {}. job: {}, broker: {} ",
                        filteredFileStatuses.size(), groupNum, entry.getKey(), groupFileSize, callback.getCallbackId(),
                        brokerDesc.getStorageType() == StorageBackend.StorageType.BROKER ? BrokerUtil.getAddress(
                                brokerDesc) : brokerDesc.getStorageType());
                groupNum++;
            }
            fileStatusMap.put(aggKey, fileStatusList);
            LOG.info("get {} files to be loaded. total size: {}. cost: {} ms, job: {}", tableTotalFileNum,
                    tableTotalFileSize, (System.currentTimeMillis() - start), callback.getCallbackId());
        }
    }

    private void beginCopy(BrokerPendingTaskAttachment attachment) throws DdlException {
        CopyJob copyJob = (CopyJob) callback;
        long startTime = System.currentTimeMillis();
        long timeoutTime = startTime + copyJob.getTimeout() * 1000 + 5000; // add a delta

        long totalFileSize = 0;
        int totalFileNum = 0;
        for (Entry<FileGroupAggKey, List<List<Pair<TBrokerFileStatus, ObjectFilePB>>>> entry :
                fileStatusMap.entrySet()) {
            FileGroupAggKey fileGroupAggKey = entry.getKey();
            List<List<Pair<TBrokerFileStatus, ObjectFilePB>>> value = entry.getValue();

            List<ObjectFilePB> objectFiles = value.stream().flatMap(List::stream).map(l -> l.second)
                    .collect(Collectors.toList());
            // groupId is 0 because the tableId is unique in FileGroupAggKey(copy into can't set partition now)
            List<ObjectFilePB> filteredObjectFiles = Env.getCurrentInternalCatalog()
                    .beginCopy(copyJob.getStageId(), copyJob.getStageType(), fileGroupAggKey.getTableId(),
                            copyJob.getCopyId(), 0, startTime, timeoutTime, objectFiles);
            Set<String> filteredObjectSet = filteredObjectFiles.stream().map(f -> f.getKey() + "-" + f.getEtag())
                    .collect(Collectors.toSet());
            LOG.debug("Begin copy for stage={}, table={}, before objectSize={}, filtered objectSize={}",
                    copyJob.getStageId(), fileGroupAggKey.getTableId(), objectFiles.size(), filteredObjectSet.size());

            List<List<TBrokerFileStatus>> fileStatusList = new ArrayList<>();
            long tableTotalFileSize = 0;
            int tableTotalFileNum = 0;
            int groupNum = 0;

            boolean needFilter = filteredObjectFiles.size() != objectFiles.size();
            for (List<Pair<TBrokerFileStatus, ObjectFilePB>> pairs : value) {
                List<TBrokerFileStatus> fileStatuses = new ArrayList<>();
                for (Pair<TBrokerFileStatus, ObjectFilePB> pair : pairs) {
                    TBrokerFileStatus brokerFileStatus = pair.first;
                    ObjectFilePB objectFile = pair.second;
                    if (!needFilter || filteredObjectSet.contains(objectFile.getKey() + "-" + objectFile.getEtag())) {
                        tableTotalFileSize += brokerFileStatus.getSize();
                        tableTotalFileNum++;
                        fileStatuses.add(brokerFileStatus);
                    }
                }
                fileStatusList.add(fileStatuses);
                LOG.info("get {} files in file group {} for table {}. size: {}. job: {}", fileStatuses.size(), groupNum,
                        entry.getKey(), tableTotalFileSize, callback.getCallbackId());
                groupNum++;
            }

            totalFileSize += tableTotalFileSize;
            totalFileNum += tableTotalFileNum;
            attachment.addFileStatus(fileGroupAggKey, fileStatusList);
            LOG.info("get {} files to be loaded. total size: {}. cost: {} ms, job: {}", tableTotalFileNum,
                    tableTotalFileSize, (System.currentTimeMillis() - startTime), callback.getCallbackId());
        }
        copyJob.setLoadFileInfo(totalFileNum, totalFileSize);
    }

}
