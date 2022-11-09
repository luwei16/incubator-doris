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

suite("test_parquet", "p0") {
     def externalStageName = "regression_test_customer"
     def filePrefix = "copy_into/parquet"

     sql """
         create stage if not exists ${externalStageName} 
         properties ('endpoint' = '${getS3Endpoint()}' ,
         'region' = '${getS3Region()}' ,
         'bucket' = '${getS3BucketName()}' ,
         'prefix' = 'regression' ,
         'ak' = '${getS3AK()}' ,
         'sk' = '${getS3SK()}' ,
         'provider' = '${getProvider()}',
         'default.file.column_separator' = "|");
     """

     def tables = [
                   "part",
                   "upper_case",
                   "reverse",
                   "set1",
                   "set2",
                   "set3",
                   "set4",
                   "set5",
                   "set6",
                   "set7",
                   "null_default",
                   "filter",
                   "s3_case1", // col1 not in file but in table, will throw "col not found" error
                   "s3_case2", // x1 not in file, not in table, will throw "col not found" error.
                   "s3_case3", // p_comment not in table but in file, load normally.
                   "s3_case4", // all columns are in table but not in file, will throw "no column found" error.
                   "s3_case5", // x1 not in file, not in table, will throw "col not found" error.
                   "s3_case6", // normal
                  ]
     def columns_list = [ 
                          """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment""",
                          """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment""",
                          """p_comment, p_retailprice, p_container, p_size, p_type, p_brand, p_mfgr, p_name, p_partkey""",
                          """p_partkey, p_name, p_size, greatest(cast(p_partkey as int), cast(p_size as int))""",
                          """p_partkey + 100""",
                          """p_partkey + 100""",
                          """p_partkey + p_size""",
                          """(p_partkey + 1) * 2""",
                          """p_partkey + 1,  p_size * 2""",
                          """p_partkey + p_size,  p_size""",
                          """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment""",
                          """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment""",
                          """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, col1""",
                          """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, x1""",
                          """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment""",
                          """col1, col2, col3, col4""",
                          """p_partkey, p_name, p_mfgr, x1""",
                          """p_partkey, p_name, p_mfgr, p_brand""",
                        ]

     def where_exprs = [
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "where p_partkey>10",
                        "",
                        "",
                        "",
                        "",
                        "",
                        ""
                        ]

     def load_rows = [
                     "100490",
                     "100490",
                     "100490",
                     "100490",
                     "100490",
                     "100490",
                     "100490",
                     "100490",
                     "100490",
                     "100490",
                     "100490",
                     "100480",
                     "100490",
                     "100490",
                     "100490",
                     "100490",
                     "100490",
                     "100490"
                     ]

     def error_msg = [
                     "",
                     "",
                     "",
                     "",
                     "",
                     "",
                     "",
                     "",
                     "",
                     "",
                     "",
                     "",
                     "errCode = 2, detailMessage = some cloumns not found in the file",
                     "errCode = 2, detailMessage = some cloumns not found in the file",
                     "",
                     "errCode = 2, detailMessage = some cloumns not found in the file",
                     "errCode = 2, detailMessage = some cloumns not found in the file",
                     ""
                     ]


     // set fe configuration
     sql "ADMIN SET FRONTEND CONFIG ('max_bytes_per_broker_scanner' = '161061273600')"

     def do_copy_into = {table, columns, stageName, prefix, where_expr ->
         sql """
             copy into $table
             from (select $columns from @${stageName}('${prefix}/part.parquet')  $where_expr)
             properties ('file.type' = 'parquet', 'copy.async' = 'false');
             """
     }

     def i = 0
     def result;
     for (String table in tables) {
         sql new File("""${context.file.parent}/ddl/${table}_drop.sql""").text
         sql new File("""${context.file.parent}/ddl/${table}_create.sql""").text

         result = do_copy_into.call(table, columns_list[i], externalStageName, filePrefix, where_exprs[i])
         logger.info("i: " + i + ", copy result: " + result)
         assertTrue(result.size() == 1)
         if (result[0][1].equals("FINISHED")) {
             assertTrue(result[0][4].equals(load_rows[i]), "expected: " + load_rows[i] + ", actual: " + result[0][4])
         }
         if (result[0][1].equals("CANCELLED")) {
             assertTrue(error_msg[i] == result[0][3], "expected: " + error_msg[i] + ", actual: " + result[0][3])
         }
         i++
     }

     // file columns size =  table columns size
     // select *
     sql new File("""${context.file.parent}/ddl/part_drop.sql""").text
     sql new File("""${context.file.parent}/ddl/part_create.sql""").text
     result = sql """ copy into part 
                      from (select *  from @${externalStageName}('${filePrefix}/part.parquet'))
                      properties ('file.type' = 'parquet', 'copy.async' = 'false');
                  """
     logger.info("copy result: " + result)
     assertTrue(result[0][1].equals("FINISHED"), "Finish copy into, state=" + result[0][1] + ", expected state=FINISHED")
     assertTrue(result[0][4].equals("100490"), "expected: 100490, actual: " + result[0][4])

     // no select
     sql new File("""${context.file.parent}/ddl/part_drop.sql""").text
     sql new File("""${context.file.parent}/ddl/part_create.sql""").text
     result = sql """ copy into part
                       from @${externalStageName}('${filePrefix}/part.parquet')
                       properties ('file.type' = 'parquet', 'copy.async' = 'false');
                   """
     logger.info("copy result: " + result)
     assertTrue(result[0][1].equals("FINISHED"), "Finish copy into, state=" + result[0][1] + ", expected state=FINISHED")
     assertTrue(result[0][4].equals("100490"), "expected: 100490, actual: " +  result[0][4])

     // file coumns size > table columns size
     // select *
     sql new File("""${context.file.parent}/ddl/s3_case3_drop.sql""").text
     sql new File("""${context.file.parent}/ddl/s3_case3_create.sql""").text
     result = sql """ copy into s3_case3
                      from (select * from @${externalStageName}('${filePrefix}/part.parquet'))
                      properties ('file.type' = 'parquet', 'copy.async' = 'false');
                  """
     logger.info("copy result: " + result)
     assertTrue(result[0][1].equals("FINISHED"), "Finish copy into, state=" + result[0][1] + ", expected state=FINISHED")
     assertTrue(result[0][4].equals("100490"), "expected: 100490, actual: " + result[0][4])

     // no select
     sql new File("""${context.file.parent}/ddl/s3_case3_drop.sql""").text
     sql new File("""${context.file.parent}/ddl/s3_case3_create.sql""").text
     result = sql """ copy into s3_case3
                      from @${externalStageName}('${filePrefix}/part.parquet')
                      properties ('file.type' = 'parquet', 'copy.async' = 'false');
                  """
     logger.info("copy result: " + result)
     assertTrue(result[0][1].equals("FINISHED"), "Finish copy into, state=" + result[0][1] + ", expected state=FINISHED")
     assertTrue(result[0][4].equals("100490"), "expected: 100490, actual: " + result[0][4])

     // file coumns size < table columns size
     // select *
     sql new File("""${context.file.parent}/ddl/null_default_drop.sql""").text
     sql new File("""${context.file.parent}/ddl/null_default_create.sql""").text
     result = sql """ copy into null_default 
                      from (select * from @${externalStageName}('${filePrefix}/part.parquet'))
                      properties ('file.type' = 'parquet', 'copy.async' = 'false');
                  """
     logger.info("copy result: " + result)
     assertTrue(result[0][3] == "errCode = 2, detailMessage = some cloumns not found in the file",
             "expected: errCode = 2, detailMessage = some cloumns not found in the file, actual: " + result[0][3])

     // no select
     sql new File("""${context.file.parent}/ddl/null_default_drop.sql""").text
     sql new File("""${context.file.parent}/ddl/null_default_create.sql""").text
     result = sql """ copy into null_default
                      from @${externalStageName}('${filePrefix}/part.parquet')
                      properties ('file.type' = 'parquet', 'copy.async' = 'false');
                  """
     logger.info("copy result: " + result)
     assertTrue(result[0][3] == "errCode = 2, detailMessage = some cloumns not found in the file",
             "expected: errCode = 2, detailMessage = some cloumns not found in the file, actual: " + result[0][3])

     for (String table in tables) {
         sql new File("""${context.file.parent}/ddl/${table}_drop.sql""").text
     }
}

