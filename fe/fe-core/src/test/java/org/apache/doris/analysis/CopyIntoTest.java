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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.utframe.TestWithFeService;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Lists;
import com.selectdb.cloud.proto.SelectdbCloud.StagePB;
import com.selectdb.cloud.proto.SelectdbCloud.StagePB.StageType;
import mockit.Expectations;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.List;

public class CopyIntoTest extends TestWithFeService {

    private static final String OBJ_INFO =  "(\"bucket\" = \"tmp_bucket\", "
            + "\"endpoint\" = \"cos.ap-beijing.myqcloud.com\", "
            + "\"prefix\" = \"tmp_prefix\", "
            + "\"sk\" = \"tmp_sk\", "
            + "\"ak\" = \"tmp_ak\", "
            + "\"region\" = \"ap-beijing\") ";
    private List<String> tableColumnNames = Lists.newArrayList("id", "name", "score");

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        String varcharTable = "CREATE TABLE t2 (\n" + "id INT,\n" + "name varchar(20),\n" + "score INT\n" + ")\n"
                + "DUPLICATE KEY(id, name)\n" + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1');";
        createTable(varcharTable);
    }

    @Test
    public void testCopyInto() throws Exception {
        String query = "create stage if not exists ex_stage_1 " + OBJ_INFO
                + "file_format = ('type' = 'csv', 'column_separator'=\",\") "
                + "copy_option = ('on_error' = 'max_filter_ratio_0.4', 'size_limit' = '100')";
        StagePB stagePB = ((CreateStageStmt) UtFrameUtils.parseAndAnalyzeStmt(query, connectContext)).toStageProto();

        new Expectations(connectContext.getEnv(), connectContext.getEnv().getInternalCatalog()) {
            {
                Env.getCurrentInternalCatalog().getStage((StageType) any, anyString, "ex_stage_1");
                minTimes = 0;
                result = stagePB;
            }
        };

        String copySql = "copy into t2 from (select from '@ex_stage_1') ";
        checkEmptyDataDescription(copySql);

        copySql = "copy into t2 from (select * from '@ex_stage_1') ";
        checkEmptyDataDescription(copySql);

        copySql = "copy into t2 from (select $2, $1, $3 from '@ex_stage_1') ";
        checkDataDescription(copySql, Lists.newArrayList("$2", "$1", "$3"));

        copySql = "copy into t2 from (select $3, $1 from '@ex_stage_1') ";
        checkDataDescriptionWithException(copySql);

        copySql = "copy into t2 from (select $2, $1+100, $3 from '@ex_stage_1') ";
        checkDataDescription(copySql, Lists.newArrayList("$2", "$1", "$3"));

        copySql = "copy into t2 from "
                + "(select $1, str_to_date($3, '%Y-%m-%d'), $2 + 1 from '@ex_stage_1' where $2 > $1) ";
        checkDataDescription(copySql, Lists.newArrayList("$1", "$3", "$2"));

        copySql = "copy into t2 from "
                + "(select $1, str_to_date($3, '%Y-%m-%d'), $2 + 1 from '@ex_stage_1' where $2 > $1) ";
        checkDataDescription(copySql, Lists.newArrayList("$1", "$3", "$2"));

        copySql = "copy into t2 from (select $2, NULL, $3 from '@ex_stage_1') ";
        checkDataDescriptionWithNull(copySql, Lists.newArrayList("$2", "", "$3"), 1);
    }

    private void checkDataDescription(String sql, List<String> filedColumns) {
        try {
            CopyStmt copyStmt = (CopyStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
            System.out.println("original sql: " + sql);
            System.out.println("parsed sql: " + copyStmt.toSql());
            List<DataDescription> dataDescriptions = copyStmt.getDataDescriptions();
            Assert.assertEquals(1, dataDescriptions.size());
            DataDescription dataDescription = dataDescriptions.get(0);
            // check file field names
            List<String> fileFieldNames = dataDescription.getFileFieldNames();
            Assert.assertEquals(3, fileFieldNames.size());
            for (int i = 0; i < fileFieldNames.size(); i++) {
                Assert.assertEquals("$" + (i + 1), fileFieldNames.get(i));
            }
            // check column mapping
            List<Expr> columnMappingList = dataDescription.getColumnMappingList();
            Assert.assertNotNull(columnMappingList);
            Assert.assertEquals(3, columnMappingList.size());
            for (int i = 0; i < columnMappingList.size(); i++) {
                Expr expr = columnMappingList.get(i);
                System.out.println("expr = " + expr.debugString());
                List<SlotRef> slotRefs = Lists.newArrayList();
                Expr.collectList(Lists.newArrayList(expr), SlotRef.class, slotRefs);
                Assert.assertEquals(2, slotRefs.size());
                Assert.assertEquals(tableColumnNames.get(i), slotRefs.get(0).getColumnName());
                Assert.assertEquals(filedColumns.get(i), slotRefs.get(1).getColumnName());
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("must be success.");
        }
    }

    private void checkDataDescriptionWithNull(String sql, List<String> filedColumns, int nullId) {
        try {
            CopyStmt copyStmt = (CopyStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
            System.out.println("original sql: " + sql);
            System.out.println("parsed sql: " + copyStmt.toSql());
            List<DataDescription> dataDescriptions = copyStmt.getDataDescriptions();
            Assert.assertEquals(1, dataDescriptions.size());
            DataDescription dataDescription = dataDescriptions.get(0);
            // check file field names
            List<String> fileFieldNames = dataDescription.getFileFieldNames();
            Assert.assertEquals(3, fileFieldNames.size());
            for (int i = 0; i < fileFieldNames.size(); i++) {
                Assert.assertEquals("$" + (i + 1), fileFieldNames.get(i));
            }
            // check column mapping
            List<Expr> columnMappingList = dataDescription.getColumnMappingList();
            Assert.assertNotNull(columnMappingList);
            Assert.assertEquals(3, columnMappingList.size());
            for (int i = 0; i < columnMappingList.size(); i++) {
                Expr expr = columnMappingList.get(i);
                System.out.println("expr = " + expr.debugString());
                List<SlotRef> slotRefs = Lists.newArrayList();
                Expr.collectList(Lists.newArrayList(expr), SlotRef.class, slotRefs);
                if (i == nullId) {
                    Assert.assertEquals(1, slotRefs.size());
                    Assert.assertEquals(tableColumnNames.get(i), slotRefs.get(0).getColumnName());
                } else {
                    Assert.assertEquals(2, slotRefs.size());
                    Assert.assertEquals(tableColumnNames.get(i), slotRefs.get(0).getColumnName());
                    Assert.assertEquals(filedColumns.get(i), slotRefs.get(1).getColumnName());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("must be success.");
        }
    }

    private void checkEmptyDataDescription(String sql) {
        try {
            CopyStmt copyStmt = (CopyStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
            System.out.println("original sql: " + sql);
            System.out.println("parsed sql: " + copyStmt.toSql());
            List<DataDescription> dataDescriptions = copyStmt.getDataDescriptions();
            Assert.assertEquals(1, dataDescriptions.size());
            DataDescription dataDescription = dataDescriptions.get(0);
            // check file field names
            Assert.assertNull(dataDescription.getFileFieldNames());
            Assert.assertNull(dataDescription.getPrecdingFilterExpr());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("must be success.");
        }
    }

    private void checkDataDescriptionWithException(String sql) {
        do {
            try {
                UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
                Assert.fail("should not come here");
            } catch (AnalysisException e) {
                Assert.assertTrue(true);
                break;
            } catch (Exception e) {
                Assert.fail("must be AnalysisException.");
            }
            Assert.fail("must be AnalysisException.");
        } while (false);
    }
}
