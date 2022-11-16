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

import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CopyFromParam {
    private static final Logger LOG = LogManager.getLogger(CopyFromParam.class);
    private static final String DOLLAR = "$";

    @Getter
    private StageAndPattern stageAndPattern;
    @Getter
    private List<Expr> exprList;
    @Getter
    private Expr fileFilterExpr;
    @Getter
    private List<String> fileColumns;
    @Getter
    private List<Expr> columnMappingList;

    public CopyFromParam(StageAndPattern stageAndPattern) {
        this.stageAndPattern = stageAndPattern;
    }

    public CopyFromParam(StageAndPattern stageAndPattern, List<Expr> exprList, Expr whereExpr) {
        this.stageAndPattern = stageAndPattern;
        this.exprList = exprList;
        this.fileFilterExpr = whereExpr;
    }

    public void analyze(String fullDbName, TableName tableName, boolean useDeleteSign) throws AnalysisException {
        if (exprList == null && fileFilterExpr == null && !useDeleteSign) {
            return;
        }
        this.fileColumns = new ArrayList<>();
        this.columnMappingList = new ArrayList<>();

        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(fullDbName);
        OlapTable olapTable = db.getOlapTableOrAnalysisException(tableName.getTbl());
        List<Column> tableColumns = olapTable.getBaseSchema(false);
        if (useDeleteSign) {
            if (olapTable.getKeysType() != KeysType.UNIQUE_KEYS) {
                throw new AnalysisException("copy.use_delete_sign property only support unique table");
            }
            tableColumns.add(getDeleteSignColumn(olapTable));
        }

        if (!getFileColumnNames()) {
            int maxFileColumnId = getMaxFileColumnId();
            maxFileColumnId = tableColumns.size() > maxFileColumnId ? tableColumns.size() : maxFileColumnId;
            for (int i = 1; i <= maxFileColumnId; i++) {
                fileColumns.add(DOLLAR + i);
            }
        }

        if (exprList != null) {
            for (int i = 0; i < exprList.size() && i < tableColumns.size(); i++) {
                Expr expr = exprList.get(i);
                String name = tableColumns.get(i).getName();
                BinaryPredicate binaryPredicate = new BinaryPredicate(Operator.EQ, new SlotRef(null, name), expr);
                columnMappingList.add(binaryPredicate);
            }
        } else {
            for (int i = 0; i < tableColumns.size(); i++) {
                String name = tableColumns.get(i).getName();
                BinaryPredicate binaryPredicate = new BinaryPredicate(Operator.EQ, new SlotRef(null, name),
                        new SlotRef(null, fileColumns.get(i)));
                columnMappingList.add(binaryPredicate);
            }
        }
    }

    // expr use column name, not use $
    private boolean getFileColumnNames() throws AnalysisException {
        if (exprList == null) {
            return false;
        }
        List<SlotRef> slotRefs = Lists.newArrayList();
        Expr.collectList(exprList, SlotRef.class, slotRefs);
        Set<String> columnSet = new HashSet<String>();
        for (SlotRef slotRef : slotRefs) {
            String columnName = slotRef.getColumnName();
            if (columnName.startsWith(DOLLAR)) {
                if (fileColumns.size() > 0) {
                    throw new AnalysisException("can not mix column name and $");
                }
                return false;
            }
            if (!columnSet.contains(columnName)) {
                columnSet.add(columnName);
                fileColumns.add(columnName);
            }
        }
        return true;
    }

    private Column getDeleteSignColumn(OlapTable olapTable) throws AnalysisException {
        for (Column column : olapTable.getFullSchema()) {
            if (column.isDeleteSignColumn()) {
                return column;
            }
        }
        throw new AnalysisException("Can not find DeleteSignColumn for unique table");
    }

    private int getMaxFileColumnId() throws AnalysisException {
        int maxId = 0;
        if (exprList != null) {
            int maxFileColumnId = getMaxFileColumnId(exprList);
            maxId = maxId > maxFileColumnId ? maxId : maxFileColumnId;
        }
        if (fileFilterExpr != null) {
            int maxFileColumnId = getMaxFileColumnId(Lists.newArrayList(fileFilterExpr));
            maxId = maxId > maxFileColumnId ? maxId : maxFileColumnId;
        }
        return maxId;
    }

    private int getMaxFileColumnId(List<Expr> exprList) throws AnalysisException {
        List<SlotRef> slotRefs = Lists.newArrayList();
        Expr.collectList(exprList, SlotRef.class, slotRefs);
        int maxId = 0;
        for (SlotRef slotRef : slotRefs) {
            int fileColumnId = getFileColumnIdOfSlotRef(slotRef);
            maxId = fileColumnId < maxId ? maxId : fileColumnId;
        }
        return maxId;
    }

    private int getFileColumnIdOfSlotRef(SlotRef slotRef) throws AnalysisException {
        String columnName = slotRef.getColumnName();
        try {
            if (!columnName.startsWith(DOLLAR)) {
                throw new AnalysisException("can not mix column name and $");
            }
            return Integer.parseInt(columnName.substring(1));
        } catch (NumberFormatException e) {
            throw new AnalysisException("column name: " + columnName + " can not parse to a number");
        }
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        if (columnMappingList != null || fileFilterExpr != null) {
            sb.append("(SELECT ");
            if (columnMappingList != null) {
                Joiner.on(", ").appendTo(sb,
                        Lists.transform(columnMappingList, (Function<Expr, Object>) expr -> expr.toSql()));
            }
            sb.append(" FROM ").append(stageAndPattern.toSql());
            if (fileFilterExpr != null) {
                sb.append(" WHERE ").append(fileFilterExpr.toSql());
            }
            sb.append(")");
        } else {
            sb.append(stageAndPattern.toSql());
        }
        return sb.toString();
    }
}
