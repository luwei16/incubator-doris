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
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.io.Text;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.FailMsg;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.thrift.TBrokerFileStatus;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.selectdb.cloud.proto.SelectdbCloud.StagePB;
import com.selectdb.cloud.storage.RemoteBase.ObjectInfo;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class CopyJob extends BrokerLoadJob {
    private static final Logger LOG = LogManager.getLogger(CopyJob.class);

    @Getter
    private String stageId;
    @Getter
    private StagePB.StageType stageType;
    @Getter
    private long sizeLimit;
    @Getter
    private String pattern;
    @Getter
    private ObjectInfo objectInfo;
    @Getter
    private String copyId;
    private String loadFilePaths = "";

    public CopyJob() {
        super(EtlJobType.COPY);
    }

    public CopyJob(long dbId, String label, BrokerDesc brokerDesc, OriginStatement originStmt, UserIdentity userInfo,
            String stageId, StagePB.StageType stageType, long sizeLimit, String pattern,
            ObjectInfo objectInfo) throws MetaNotFoundException {
        super(EtlJobType.COPY, dbId, label, brokerDesc, originStmt, userInfo);
        this.stageId = stageId;
        this.stageType = stageType;
        this.sizeLimit =  sizeLimit;
        this.pattern = pattern;
        this.objectInfo = objectInfo;
        // FIXME(meiyi): specify copy id use: 1. label; 2. auto_increment job id from meta service...
        this.copyId = label;
    }

    @Override
    protected LoadTask createPendingTask() {
        return new CopyLoadPendingTask(this, fileGroupAggInfo.getAggKeyToFileGroups(), brokerDesc);
    }

    @Override
    protected void afterCommit() throws DdlException {
        super.afterCommit();
        for (Entry<FileGroupAggKey, List<BrokerFileGroup>> entry : fileGroupAggInfo.getAggKeyToFileGroups()
                .entrySet()) {
            long tableId = entry.getKey().getTableId();
            LOG.debug("Start finish copy for stage={}, table={}", stageId, tableId);
            Env.getCurrentInternalCatalog().finishCopy(stageId, stageType, tableId, getCopyId(), 0, true);
        }
    }

    @Override
    public void cancelJob(FailMsg failMsg) throws DdlException {
        super.cancelJob(failMsg);
        for (Entry<FileGroupAggKey, List<BrokerFileGroup>> entry : fileGroupAggInfo.getAggKeyToFileGroups()
                .entrySet()) {
            long tableId = entry.getKey().getTableId();
            LOG.debug("Start cancel copy for stage={}, table={}", stageId, tableId);
            Env.getCurrentInternalCatalog().finishCopy(stageId, stageType, tableId, getCopyId(), 0, false);
        }
    }

    @Override
    public void cancelJobWithoutCheck(FailMsg failMsg, boolean abortTxn, boolean needLog) {
        super.cancelJobWithoutCheck(failMsg, abortTxn, needLog);
        for (Entry<FileGroupAggKey, List<BrokerFileGroup>> entry : fileGroupAggInfo.getAggKeyToFileGroups()
                .entrySet()) {
            long tableId = entry.getKey().getTableId();
            LOG.debug("Start cancel copy for stage={}, table={}", stageId, tableId);
            try {
                Env.getCurrentInternalCatalog().finishCopy(stageId, stageType, tableId, "copy" + label, 0, false);
            } catch (DdlException e) {
                // if cancel copy failed, kvs in fdb will be cleaned when expired
                LOG.warn("Failed to cancel copy for stage={}, table={}", stageId, tableId, e);
            }
        }
    }

    @Override
    protected List<Comparable> getShowInfoUnderLock() throws DdlException {
        List<Comparable> showInfos = new ArrayList<>();
        showInfos.add(getCopyId());
        showInfos.addAll(super.getShowInfoUnderLock());
        showInfos.add(loadFilePaths);
        return showInfos;
    }

    @Override
    protected void logFinalOperation() {
        Env.getCurrentEnv().getEditLog().logEndLoadJob(getLoadJobFinalOperation());
    }

    @Override
    public void unprotectReadEndOperation(LoadJobFinalOperation loadJobFinalOperation) {
        super.unprotectReadEndOperation(loadJobFinalOperation);
        this.copyId = loadJobFinalOperation.getCopyId();
        this.loadFilePaths = loadJobFinalOperation.getLoadFilePaths();
    }

    @Override
    protected LoadJobFinalOperation getLoadJobFinalOperation() {
        return new LoadJobFinalOperation(id, loadingStatus, progress, loadStartTimestamp,
                finishTimestamp, state, failMsg, copyId, loadFilePaths);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, copyId);
        Text.writeString(out, loadFilePaths);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        copyId = Text.readString(in);
        loadFilePaths = Text.readString(in);
    }

    protected void setSelectedFiles(Map<FileGroupAggKey, List<List<TBrokerFileStatus>>> fileStatusMap) {
        this.loadFilePaths = selectedFilesToJson(fileStatusMap);
    }

    private String selectedFilesToJson(Map<FileGroupAggKey, List<List<TBrokerFileStatus>>> selectedFiles) {
        if (selectedFiles == null) {
            return "";
        }
        List<String> paths = new ArrayList<>();
        for (Entry<FileGroupAggKey, List<List<TBrokerFileStatus>>> entry : selectedFiles.entrySet()) {
            for (List<TBrokerFileStatus> fileStatuses : entry.getValue()) {
                paths.addAll(fileStatuses.stream().map(e -> e.path).collect(Collectors.toList()));
            }
        }
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(paths);
    }
}
