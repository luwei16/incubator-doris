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

import org.apache.doris.backup.S3Storage;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.load.loadv2.LoadTask.MergeType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.collect.Lists;
import com.selectdb.cloud.proto.SelectdbCloud.ObjectStoreInfoPB;
import com.selectdb.cloud.proto.SelectdbCloud.StagePB;
import com.selectdb.cloud.proto.SelectdbCloud.StagePB.StageType;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Copy statement
 */
public class CopyStmt extends DdlStmt {
    private static final Logger LOG = LogManager.getLogger(CopyStmt.class);

    private static final ShowResultSetMetaData COPY_INTO_META_DATA =
            ShowResultSetMetaData.builder()
                .addColumn(new Column("copyId", ScalarType.createVarchar(64)))
                .addColumn(new Column("state", ScalarType.createVarchar(64)))
                .addColumn(new Column("type", ScalarType.createVarchar(64)))
                .addColumn(new Column("msg", ScalarType.createVarchar(128)))
                .addColumn(new Column("loadedRows", ScalarType.createVarchar(64)))
                .addColumn(new Column("filterRows", ScalarType.createVarchar(64)))
                .addColumn(new Column("unselectRows", ScalarType.createVarchar(64)))
                .addColumn(new Column("url", ScalarType.createVarchar(128)))
            .build();
    public static final String S3_BUCKET = "bucket";
    public static final String S3_PREFIX = "prefix";

    @Getter
    private final TableName tableName;
    @Getter
    private CopyFromParam copyFromParam;
    @Getter
    private String stage;
    @Getter
    private FilesOrPattern filesOrPattern;
    @Getter
    private FileFormat fileFormat;
    @Getter
    private CopyOption copyOption;
    @Getter
    private boolean async;

    private LabelName label = null;
    private BrokerDesc brokerDesc = null;
    private DataDescription dataDescription = null;
    private final Map<String, String> brokerProperties = new HashMap<>();
    private final Map<String, String> properties = new HashMap<>();
    @Getter
    private String stageId;
    @Getter
    private StageType stageType;
    @Getter
    private long sizeLimit;

    /**
     * Use for cup.
     */
    public CopyStmt(TableName tableName, CopyFromParam copyFromParam, FilesOrPattern filesOrPattern,
            FileFormat fileFormat, CopyOption copyOption, boolean async) {
        this.tableName = tableName;
        this.copyFromParam = copyFromParam;
        this.stage = copyFromParam.getStage();
        this.filesOrPattern = filesOrPattern;
        this.fileFormat = fileFormat;
        this.copyOption = copyOption;
        this.async = async;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        // generate a label
        String labelName = "copy_" + DebugUtil.printId(analyzer.getContext().queryId()).replace("-", "_");
        label = new LabelName(tableName.getDb(), labelName);
        label.analyze(analyzer);
        // analyze stage
        analyzeStageName();
        // get stage from meta service. And permission is checked in meta service
        StagePB stagePB;
        String user = ClusterNamespace.getNameFromFullName(
                ConnectContext.get().getCurrentUserIdentity().getQualifiedUser());
        if (stage.equals("~")) {
            stagePB = Env.getCurrentInternalCatalog().getStage(StageType.INTERNAL, user, null);
        } else {
            stagePB = Env.getCurrentInternalCatalog().getStage(StageType.EXTERNAL, user, stage);
        }
        analyzeStagePB(stagePB);

        // generate broker desc
        sizeLimit = copyOption.getSizeLimit();
        brokerDesc = new BrokerDesc("S3", StorageBackend.StorageType.S3, brokerProperties);
        // generate data description
        String filePath = "s3://" + brokerProperties.get(S3_BUCKET) + "/" + brokerProperties.get(S3_PREFIX);
        Separator separator = fileFormat.getColumnSeparator() != null ? new Separator(fileFormat.getColumnSeparator())
                : null;
        String fileFormatStr = fileFormat.getFormat();
        Map<String, String> dataDescProperties = new HashMap<>();
        fileFormat.toDataDescriptionProperties(dataDescProperties);
        copyFromParam.analyze(label.getDbName(), tableName);
        if (copyFromParam.isSelect()) {
            dataDescription = new DataDescription(tableName.getTbl(), null, Lists.newArrayList(filePath),
                    copyFromParam.getFileColumns(), separator, fileFormatStr, null, false,
                    copyFromParam.getColumnMappingList(), copyFromParam.getFileFilterExpr(),
                    null, MergeType.APPEND, null, null, dataDescProperties);
        } else {
            dataDescription = new DataDescription(tableName.getTbl(), null, Lists.newArrayList(filePath), null,
                    separator, fileFormatStr, null, false, null, null, null, MergeType.APPEND, null, null,
                    dataDescProperties);
        }
        // analyze data description
        dataDescription.analyze(label.getDbName());
        for (int i = 0; i < dataDescription.getFilePaths().size(); i++) {
            dataDescription.getFilePaths().set(i, brokerDesc.convertPathToS3(dataDescription.getFilePaths().get(i)));
            dataDescription.getFilePaths()
                    .set(i, ExportStmt.checkPath(dataDescription.getFilePaths().get(i), brokerDesc.getStorageType()));
        }

        try {
            // TODO support exec params as LoadStmt
            LoadStmt.checkProperties(properties);
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    private void analyzeStageName() throws AnalysisException {
        if (stage.isEmpty()) {
            throw new AnalysisException("Stage name can not be empty");
        }
    }

    // after analyzeStagePB, fileFormat and copyOption is not null
    private void analyzeStagePB(StagePB stagePB) throws AnalysisException {
        ObjectStoreInfoPB objInfo = stagePB.getObjInfo();
        brokerProperties.put(S3Storage.S3_ENDPOINT, "http://" + objInfo.getEndpoint());
        // brokerProperties.put(S3Storage.S3_ENDPOINT, objInfo.getEndpoint());
        brokerProperties.put(S3Storage.S3_REGION, objInfo.getRegion());
        brokerProperties.put(S3Storage.S3_AK, objInfo.getAk());
        brokerProperties.put(S3Storage.S3_SK, objInfo.getSk());
        brokerProperties.put(S3_BUCKET, objInfo.getBucket());
        brokerProperties.put(S3_PREFIX, objInfo.getPrefix());

        stageType = stagePB.getType();
        stageId = stagePB.getStageId();
        Map<String, String> fileFormatProperties = stagePB.getFileFormatPropertiesMap();
        if (this.fileFormat == null) {
            this.fileFormat = new FileFormat(fileFormatProperties);
        } else {
            this.fileFormat.mergeProperties(fileFormatProperties);
        }
        if (this.fileFormat.getProperties().size() > 0) {
            this.fileFormat.analyze();
        }
        Map<String, String> copyOptionProperties = stagePB.getCopyOptionPropertiesMap();
        if (this.copyOption == null) {
            this.copyOption = new CopyOption(copyOptionProperties);
        } else {
            this.copyOption.mergeProperties(copyOptionProperties);
        }
        if (this.copyOption.getProperties().size() > 0) {
            this.copyOption.analyze();
            this.copyOption.analyzeProperty(properties);
        }
    }

    public String getDbName() {
        return label.getDbName();
    }

    public BrokerDesc getBrokerDesc() {
        return brokerDesc;
    }

    public List<DataDescription> getDataDescriptions() {
        return Lists.newArrayList(dataDescription);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public LabelName getLabel() {
        return label;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("COPY INTO ").append(tableName.toSql()).append(" \n");
        sb.append("from ").append(copyFromParam.toSql()).append("\n");
        if (filesOrPattern != null) {
            sb.append(filesOrPattern.toSql()).append(" \n");
        }
        if (fileFormat != null) {
            sb.append(fileFormat.toSql()).append(" \n");
        }
        if (copyOption != null) {
            sb.append(copyOption.toSql()).append(" \n");
        }
        if (async) {
            sb.append(" ASYNC = true");
        }
        return sb.toString();
    }

    public ShowResultSetMetaData getMetaData() {
        return COPY_INTO_META_DATA;
    }
}
