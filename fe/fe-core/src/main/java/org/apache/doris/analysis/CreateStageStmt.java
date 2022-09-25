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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.selectdb.cloud.proto.SelectdbCloud.StagePB;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Create stage.
 */
public class CreateStageStmt extends DdlStmt {
    private static final Logger LOG = LogManager.getLogger(CreateStageStmt.class);

    @Getter
    private final boolean ifNotExists;
    @Getter
    private final String stageName;
    @Getter
    private StageParam stageParam;
    @Getter
    private FileFormat fileFormat;
    @Getter
    private CopyOption copyOption;

    /**
     * Use for cup.
     */
    public CreateStageStmt(boolean ifNotExists, String stageName, StageParam stageParam,
            FileFormat fileFormat, CopyOption copyOption) {
        this.ifNotExists = ifNotExists;
        this.stageName = stageName;
        this.stageParam = stageParam;
        this.fileFormat = fileFormat;
        this.copyOption = copyOption;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        // check auth
        if (!Env.getCurrentEnv().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
        // check stage name
        if (stageName.contains("@") || stageName.contains("~") || stageName.contains("%")) {
            throw new AnalysisException("Stage name='" + stageName + "', can not include '@', '~' or '%'");
        }
        if (stageParam != null) {
            stageParam.analyze();
        }
        if (fileFormat != null) {
            fileFormat.analyze();
        }
        // check copy_option: max_file_ratio and size_limit
        if (this.copyOption != null) {
            this.copyOption.analyze();
        }
    }

    public StagePB toStageProto() throws DdlException {
        StagePB.Builder stageBuilder = StagePB.newBuilder();
        stageBuilder.addMysqlUserName(
                ClusterNamespace.getNameFromFullName(ConnectContext.get().getCurrentUserIdentity().getQualifiedUser()));
        switch (getStageParam().getType()) {
            case EXTERNAL:
                stageBuilder.setName(getStageName()).setType(StagePB.StageType.EXTERNAL)
                        .setObjInfo(getStageParam().toProto());
                break;
            case INTERNAL:
            default:
                throw new DdlException("Cant not create stage with type=" + getStageParam().getType());
        }
        if (getFileFormat() != null) {
            stageBuilder.putAllFileFormatProperties(getFileFormat().getProperties());
        }
        if (getCopyOption() != null) {
            stageBuilder.putAllCopyOptionProperties(getCopyOption().getProperties());
        }
        return stageBuilder.build();
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE STAGE ");
        if (ifNotExists) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(stageName).append(" ");
        sb.append(stageParam.toSql());
        if (fileFormat != null) {
            sb.append(fileFormat.toSql());
        }
        if (copyOption != null) {
            sb.append(copyOption.toSql());
        }
        return sb.toString();
    }
}
