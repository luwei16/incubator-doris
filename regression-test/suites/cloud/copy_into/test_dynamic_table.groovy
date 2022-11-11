suite("test_dynamic_table") {
    def tableName = "es_nested"
    def externalStageName = "dynamic_table"
    def prefix = "copy_into_dynamic_table"

    try {
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
                qid bigint,
                creationDate datetime,
                `answers.date` array<datetime>,
                `title` string,
		        ...
        )
        DUPLICATE KEY(`qid`)
        DISTRIBUTED BY RANDOM BUCKETS 5 
        properties("replication_num" = "1");
        """

        sql """
            create stage if not exists ${externalStageName} 
            properties ('endpoint' = '${getS3Endpoint()}' ,
            'region' = '${getS3Region()}' ,
            'bucket' = '${getS3BucketName()}' ,
            'ak' = '${getS3AK()}' ,
            'sk' = '${getS3SK()}' ,
            'provider' = '${getProvider()}' , 
            'default.file.type' = "json");
        """

        def result = sql " copy into ${tableName} from @${externalStageName}('${prefix}/es_nested.json') properties ('file.type' = 'json', 'copy.async' = 'false'); "
        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        assertTrue(result[0][1].equals("FINISHED"), "Finish copy into, state=" + result[0][1] + ", expected state=FINISHED")
        qt_sql " SELECT COUNT(*) FROM ${tableName}; "

        result = sql " copy into ${tableName} from @${externalStageName}('${prefix}/es_nested.json') properties ('file.type' = 'json', 'copy.async' = 'false'); "
        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        assertTrue(result[0][1].equals("CANCELLED"), "Finish copy into, state=" + result[0][1] + ", expected state=CANCELLED")
        qt_sql " SELECT COUNT(*) FROM ${tableName}; "
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}