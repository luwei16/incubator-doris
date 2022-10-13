suite("test_copy_with_select") {
    def tableName = "customer_copy_with_select"
    def externalStageName = "regression_test_copy_stage"
    def prefix = "regression/tpch/sf1"

    def getProvider = { endpoint ->
        def providers = ["cos", "oss", "s3", "obs", "bos"]
        for (final def provider in providers) {
            if (endpoint.containsIgnoreCase(provider)) {
                return provider
            }
        }
        return ""
    }

    sql """
        create stage if not exists ${externalStageName} 
        ('endpoint' = '${getS3Endpoint()}' ,
        'region' = '${getS3Region()}' ,
        'bucket' = '${getS3BucketName()}' ,
        'prefix' = '${prefix}' ,
        'ak' = '${getS3AK()}' ,
        'sk' = '${getS3SK()}' ,
        'provider' = '${getProvider(getS3Endpoint())}')
        with file_format = ('column_separator' = "|" )
        copy_option = ('on_error'='max_filter_ratio_0.4');
    """

    def sql_prefix = """ copy into ${tableName} from ("""
    def sql_stage = """  from @${externalStageName}('customer.csv.gz')"""
    def sql_postfix = """) with file_format = ('type' = 'null') async = false;"""

    def sqls = [
            'select $1, $2, $3, $4, $5, $6, $7, $8 ' + sql_stage,
            'select $1, $3, $2, $4, $5, $6, $7, $8 ' + sql_stage,
            'select $1, $2, $3, $4, $5, $6, $7, NULL ' + sql_stage,
            'select $1, $2, $3, $4, NULL, $6, $7, NULL ' + sql_stage,
            'select $1, $2, $3, $4, $5, $6, $7, $8 ' + sql_stage + ' where $1 > 2000',
            'select $1 + 20000, $2, $3, $4, $5, $6, $7, $8 ' + sql_stage,
            'select $1 + 30000, $2, $3, $4, $5, $6, $7, $8 ' + sql_stage + 'where $1 > 3000',
            'select $1, $2, $3, $4, $5, $6, $7, substring($8, 2) ' + sql_stage
    ]

    for (String copySql: sqls) {
        try {
            sql """ DROP TABLE IF EXISTS ${tableName}; """
            sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        VARCHAR(40) NOT NULL,
            C_ADDRESS     VARCHAR(40) NOT NULL,
            C_NATIONKEY   INTEGER NOT NULL,
            C_PHONE       CHAR(15) NULL,
            C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
            C_MKTSEGMENT  CHAR(10) NOT NULL,
            C_COMMENT     VARCHAR(117) NULL
            )
            UNIQUE KEY(C_CUSTKEY)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
            """

            def result = sql """ ${sql_prefix} ${copySql} ${sql_postfix} """
            logger.info("copy result: " + result)
            assertTrue(result.size() == 1)
            assertTrue(result[0].size() == 8)
            assertTrue(result[0][1].equals("FINISHED"), "Finish copy into, state=" + result[0][1] + ", expected state=FINISHED")

            qt_sql " SELECT COUNT(*) FROM ${tableName}; "

            qt_sql "select * from ${tableName} order by C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT limit 20;"

        } finally {
            try_sql("DROP TABLE IF EXISTS ${tableName}")
        }
    }
}