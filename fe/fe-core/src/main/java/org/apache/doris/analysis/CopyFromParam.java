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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class CopyFromParam {
    private static final Logger LOG = LogManager.getLogger(CopyFromParam.class);
    private static final String DOLLAR = "$";

    @Getter
    private String stage;
    @Getter
    private List<Expr> exprList;
    @Getter
    private Expr fileFilterExpr;
    @Getter
    private boolean isSelect;
    @Getter
    private List<String> fileColumns = new ArrayList<>();
    @Getter
    private List<Expr> columnMappingList = new ArrayList<>();

    public CopyFromParam(String stage) {
        this.stage = stage;
        this.isSelect = false;
    }

    public CopyFromParam(String stage, List<Expr> exprList, Expr whereExpr) {
        this.stage = stage;
        this.exprList = exprList;
        this.fileFilterExpr = whereExpr;
        this.isSelect = true;
    }

    public void analyze(String fullDbName, TableName tableName) throws AnalysisException {
        if (!isSelect || (exprList == null && fileFilterExpr == null)) {
            return;
        }

        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(fullDbName);
        OlapTable olapTable = db.getOlapTableOrAnalysisException(tableName.getTbl());
        List<Column> tableColumns = olapTable.getBaseSchema();

        int maxFileColumnId = getMaxFileColumnId();
        maxFileColumnId = tableColumns.size() > maxFileColumnId ? tableColumns.size() : maxFileColumnId;
        for (int i = 1; i <= maxFileColumnId; i++) {
            fileColumns.add(DOLLAR + i);
        }

        if (exprList != null) {
            if (tableColumns.size() > exprList.size()) {
                throw new AnalysisException("select column size is less than table column size");
            }
            for (int i = 0; i < exprList.size(); i++) {
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
            return columnName.startsWith(DOLLAR) ? Integer.parseInt(columnName.substring(1)) : 0;
        } catch (NumberFormatException e) {
            throw new AnalysisException("column name: " + columnName + " can not parse to a number");
        }
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        if (isSelect) {
            sb.append("(SELECT ");
            if (columnMappingList != null) {
                Joiner.on(", ").appendTo(sb,
                        Lists.transform(columnMappingList, (Function<Expr, Object>) expr -> expr.toSql()));
            }
            sb.append(" FROM @").append(stage);
            if (fileFilterExpr != null) {
                sb.append(" WHERE ").append(fileFilterExpr.toSql());
            }
            sb.append(")");
        } else {
            sb.append("@").append(stage);
        }
        return sb.toString();
    }
}
