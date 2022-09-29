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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;
import org.apache.doris.utframe.UtFrameUtils;

import com.selectdb.cloud.proto.SelectdbCloud.StagePB;
import com.selectdb.cloud.proto.SelectdbCloud.StagePB.StageType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class StageTest extends TestWithFeService {

    @Mocked
    private Database db;
    @Mocked
    private OlapTable table;
    private ConnectContext ctx;
    private static final String OBJ_INFO =  "(\"bucket\" = \"tmp_bucket\", "
            + "\"endpoint\" = \"cos.ap-beijing.myqcloud.com\", "
            + "\"provider\" = \"cos\", "
            + "\"prefix\" = \"tmp_prefix\", "
            + "\"sk\" = \"tmp_sk\", "
            + "\"ak\" = \"tmp_ak\", "
            + "\"region\" = \"ap-beijing\") ";

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        ctx = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testCreateStageStmt() throws Exception {
        // create an internal stage
        String sql = "create stage in_stage_1";
        parseAndAnalyzeWithException(sql, "Syntax error");

        // create an external stage with no bucket
        sql = "create stage ex_stage_1 "
                + "('endpoint' = 'cos.ap-beijing.myqcloud.com', "
                + "'region' = 'ap-beijing', "
                + "'prefix' = 'tmp_prefix', "
                + "'ak'='tmp_ak', 'sk'='tmp_sk');";
        parseAndAnalyzeWithException(sql, "bucket is required for ExternalStage");

        // create stage with file format
        try {
            sql = "create stage if not exists ex_stage_1 " + OBJ_INFO
                    + "file_format = ('type' = 'csv', 'column_separator'=\",\", 'line_delimiter'=\"\t\")";
            UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
        } catch (Exception e) {
            Assert.fail("must be success.");
        }

        // create stage with unknown file format property
        sql = "create stage if not exists ex_stage_1 " + OBJ_INFO
                + "file_format = ('type' = 'csv', 'test_key'='test_value')";
        parseAndAnalyzeWithException(sql, "'test_key' is invalid in FileFormat");

        // create stage with copy option: on_error
        try {
            sql = "create stage if not exists ex_stage_1 " + OBJ_INFO
                    + "copy_option= ('on_error' = 'continue')";
            UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
        } catch (Exception e) {
            Assert.fail("must be success.");
        }

        // create stage with copy option: on_error and size_limit
        try {
            sql = "create stage if not exists ex_stage_1 " + OBJ_INFO
                    + "copy_option= ('on_error' = 'max_filter_ratio_0.4', 'size_limit' = '100')";
            StatementBase statementBase = UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
            Assert.assertTrue(statementBase instanceof CreateStageStmt);
            Assert.assertEquals(0.4, ((CreateStageStmt) statementBase).getCopyOption().getMaxFilterRatio(), 0.02);
        } catch (Exception e) {
            Assert.fail("must be success.");
        }

        // create stage with copy option: invalid max_filter_ratio
        sql = "create stage if not exists ex_stage_1 " + OBJ_INFO
                + "copy_option= ('on_error' = 'max_filter_ratio_a') ";
        parseAndAnalyzeWithException(sql, "Property on_error with invalid value max_filter_ratio_a");

        // create an external stage with file format and copy option
        try {
            sql = "create stage ex_stage_1 " + OBJ_INFO
                    + "file_format = (\"type\" = \"csv\", \"column_separator\" = \",\") "
                    + "copy_option = (\"on_error\" = \"max_filter_ratio_0.4\", \"size_limit\" = \"100\")";
            StatementBase statementBase = UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
            Assert.assertEquals(sql, statementBase.toSql().toLowerCase().trim());
        } catch (Exception e) {
            Assert.fail("must be success.");
        }

        // create stage with invalid prefix
        sql = "create stage ex_stage_1 "
                + "('endpoint' = 'cos.ap-beijing.myqcloud.com', "
                + "'region' = 'ap-beijing', "
                + "'bucket' = 'tmp_bucket', "
                + "'prefix' = '/tmp_prefix', "
                + "'provider' = 'OSS', "
                + "'ak'='tmp_ak', 'sk'='tmp_sk');";
        parseAndAnalyzeWithException(sql, "can not start or end with '/'");

        // create stage with invalid provider
        sql = "create stage ex_stage_1 "
                + "('endpoint' = 'cos.ap-beijing.myqcloud.com', "
                + "'region' = 'ap-beijing', "
                + "'bucket' = 'tmp_bucket', "
                + "'prefix' = 'tmp_prefix', "
                + "'provider' = 'abc', "
                + "'ak'='tmp_ak', 'sk'='tmp_sk');";
        parseAndAnalyzeWithException(sql, "Property provider with invalid value abc");
    }

    @Test
    public void testStagePB() throws Exception {
        String query = "create stage if not exists ex_stage_1 " + OBJ_INFO
                + "file_format = ('type' = 'csv', 'column_separator'=\",\") "
                + "copy_option = ('on_error' = 'max_filter_ratio_0.4', 'size_limit' = '100')";
        StagePB stagePB = ((CreateStageStmt) UtFrameUtils.parseAndAnalyzeStmt(query, ctx)).toStageProto();
        String query2 = "create stage if not exists ex_stage_2 " + OBJ_INFO;
        StagePB stagePB2 = ((CreateStageStmt) UtFrameUtils.parseAndAnalyzeStmt(query2, ctx)).toStageProto();

        new Expectations(ctx.getEnv(), ctx.getEnv().getInternalCatalog(), db) {
            {
                Env.getCurrentInternalCatalog().getStage((StageType) any, anyString, "ex_stage_1");
                minTimes = 0;
                result = stagePB;

                Env.getCurrentInternalCatalog().getStage((StageType) any, anyString, "ex_stage_2");
                minTimes = 0;
                result = stagePB2;

                Env.getCurrentInternalCatalog().getDbOrAnalysisException("default_cluster:db");
                minTimes = 0;
                result = db;

                db.getOlapTableOrAnalysisException("test_table");
                minTimes = 0;
                result = table;
            }
        };
        try {
            String copyQuery = "copy into db.test_table from @ex_stage_1";
            CopyStmt copyStmt = (CopyStmt) UtFrameUtils.parseAndAnalyzeStmt(copyQuery, ctx);
            System.out.println("parsed sql: " + copyStmt.toSql());
            // check file format
            FileFormat fileFormat = copyStmt.getFileFormat();
            Assert.assertNotNull(fileFormat);
            Assert.assertEquals("csv", fileFormat.getFormat());
            Assert.assertEquals(",", fileFormat.getColumnSeparator());
            // check copy option
            CopyOption copyOption = copyStmt.getCopyOption();
            Assert.assertNotNull(copyOption);
            Assert.assertEquals(100, copyOption.getSizeLimit());
            Assert.assertEquals(0.4, copyOption.getMaxFilterRatio(), 0.02);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("must be success.");
        }
        try {
            String copyQuery = "copy into db.test_table from @ ex_stage_1 "
                    + "file_format = ('type' = 'json', 'fuzzy_parse'='true', 'json_root'=\"{\") "
                    + "copy_option= ('on_error' = 'continue', 'size_limit' = '200')";
            CopyStmt copyStmt = (CopyStmt) UtFrameUtils.parseAndAnalyzeStmt(copyQuery, ctx);
            // check file format
            FileFormat fileFormat = copyStmt.getFileFormat();
            Assert.assertNotNull(fileFormat);
            Assert.assertEquals("json", fileFormat.getFormat());
            // TODO should we merge file_format_option?
            Assert.assertEquals(",", fileFormat.getColumnSeparator());
            // check copy option
            CopyOption copyOption = copyStmt.getCopyOption();
            Assert.assertNotNull(copyOption);
            Assert.assertEquals(200, copyOption.getSizeLimit());
            Assert.assertEquals(1.0, copyOption.getMaxFilterRatio(), 0.02);
            Assert.assertTrue(copyStmt.getDataDescriptions().size() == 1);
            Assert.assertEquals("{", copyStmt.getDataDescriptions().get(0).getJsonRoot());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("must be success.");
        }
        try {
            String copyQuery = "copy into db.test_table from @ex_stage_2";
            CopyStmt copyStmt = (CopyStmt) UtFrameUtils.parseAndAnalyzeStmt(copyQuery, ctx);
            // check file format
            FileFormat fileFormat = copyStmt.getFileFormat();
            Assert.assertNotNull(fileFormat);
            Assert.assertEquals("", fileFormat.toSql());
            // check copy option
            CopyOption copyOption = copyStmt.getCopyOption();
            Assert.assertNotNull(copyOption);
            Assert.assertEquals("", copyOption.toSql());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("must be success.");
        }
    }

    private void parseAndAnalyzeWithException(String sql, String errorMsg) {
        do {
            try {
                UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains(errorMsg));
                break;
            } catch (Exception e) {
                Assert.fail("must be AnalysisException.");
            }
            Assert.fail("must be AnalysisException.");
        } while (false);
    }
}
