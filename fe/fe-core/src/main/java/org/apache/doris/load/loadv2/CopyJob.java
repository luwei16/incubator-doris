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
import org.apache.doris.analysis.FilesOrPattern;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import org.apache.doris.load.FailMsg;
import org.apache.doris.qe.OriginStatement;

import com.selectdb.cloud.proto.SelectdbCloud.StagePB;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map.Entry;

public class CopyJob extends BrokerLoadJob {
    private static final Logger LOG = LogManager.getLogger(CopyJob.class);

    @Getter
    private String stageId;
    @Getter
    private StagePB.StageType stageType;
    @Getter
    private long sizeLimit;
    @Getter
    private FilesOrPattern filesOrPattern;

    public CopyJob(long dbId, String label, BrokerDesc brokerDesc, OriginStatement originStmt, UserIdentity userInfo,
            String stageId, StagePB.StageType stageType, long sizeLimit, FilesOrPattern filesOrPattern)
            throws MetaNotFoundException {
        super(dbId, label, brokerDesc, originStmt, userInfo);
        this.stageId = stageId;
        this.stageType = stageType;
        this.sizeLimit =  sizeLimit;
        this.filesOrPattern = filesOrPattern;
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

    public String getCopyId() {
        // FIXME(meiyi): specify copy id use: 1. label; 2. auto_increment job id from meta service...
        return label;
    }
}
