suite("test_external_stage") {
    def tableName = "customer_external_stage"
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

    try {
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        VARCHAR(25) NOT NULL,
            C_ADDRESS     VARCHAR(40) NOT NULL,
            C_NATIONKEY   INTEGER NOT NULL,
            C_PHONE       CHAR(15) NOT NULL,
            C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
            C_MKTSEGMENT  CHAR(10) NOT NULL,
            C_COMMENT     VARCHAR(117) NOT NULL
            )
            UNIQUE KEY(C_CUSTKEY)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """

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

        def result = sql " copy into ${tableName} from @${externalStageName}('customer.csv.gz') with file_format = ('type' = 'csv', 'compression' = 'gz') async = false; "
        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        assertTrue(result[0][1].equals("FINISHED"), "Finish copy into, state=" + result[0][1] + ", expected state=FINISHED")
        qt_sql " SELECT COUNT(*) FROM ${tableName}; "

        result = sql " copy into ${tableName} from @${externalStageName}('customer.csv.gz') with async = false; "
        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        assertTrue(result[0][1].equals("CANCELLED"), "Finish copy into, state=" + result[0][1] + ", expected state=CANCELLED")
        qt_sql " SELECT COUNT(*) FROM ${tableName}; "
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}