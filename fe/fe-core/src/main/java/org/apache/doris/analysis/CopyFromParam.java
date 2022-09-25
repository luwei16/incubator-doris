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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import lombok.Getter;

import java.util.List;

public class CopyFromParam {
    @Getter
    private String stage;
    @Getter
    private List<String> tableColumns;
    @Getter
    private List<String> fileColumns;
    @Getter
    private List<Expr> columnMappingList;
    @Getter
    private Expr fileFilterExpr;
    @Getter
    private Expr whereExpr;
    @Getter
    private boolean isSelect;

    public CopyFromParam(String stage) {
        this.stage = stage;
        this.isSelect = false;
    }

    public CopyFromParam(String stage, List<String> tableColumns, List<String> fileColumns,
            List<Expr> columnMappingList, Expr fileFilterExpr, Expr whereExpr) {
        this.stage = stage;
        this.tableColumns = tableColumns;
        this.fileColumns = fileColumns;
        this.columnMappingList = columnMappingList;
        this.fileFilterExpr = fileFilterExpr;
        this.whereExpr = whereExpr;
        this.isSelect = true;
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        if (isSelect) {
            sb.append("(SELECT FROM '").append(stage).append("'");
            if (fileColumns != null && !fileColumns.isEmpty()) {
                sb.append(" (");
                Joiner.on(", ").appendTo(sb, fileColumns).append(")");
            }
            if (columnMappingList != null && !columnMappingList.isEmpty()) {
                sb.append(" SET (");
                Joiner.on(", ")
                        .appendTo(sb, Lists.transform(columnMappingList, (Function<Expr, Object>) expr -> expr.toSql()))
                        .append(")");
            }
            if (fileFilterExpr != null) {
                sb.append(" PRECEDING FILTER ").append(fileFilterExpr.toSql());
            }
            if (whereExpr != null) {
                sb.append(" WHERE ").append(whereExpr.toSql());
            }
            sb.append(")");
        } else {
            sb.append("'").append(stage).append("'");
        }
        return sb.toString();
    }

}
