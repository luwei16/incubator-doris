import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_delete_sign") {
    def tableName = "test_delete_on"
    def filePath = "${context.config.dataPath}/cloud/copy_into/" + "test_delete_on_0.csv"
    def deleteFilePath = "${context.config.dataPath}/cloud/copy_into/" + "test_delete_on_1.csv"
    def deleteFilePrefix = "test_delete_on_"

    def uploadFile = { name, path ->
        StringBuilder strBuilder = new StringBuilder()
        strBuilder.append("""curl -u """ + context.config.feCloudHttpUser + ":" + context.config.feCloudHttpPassword)
        strBuilder.append(""" -H fileName:""" + name)
        strBuilder.append(""" -T """ + path)
        strBuilder.append(""" -L http://""" + context.config.feCloudHttpAddress + """/copy/upload""")

        String command = strBuilder.toString()
        logger.info("upload command=" + command)
        def process = command.toString().execute()
        def code = process.waitFor()
        def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
        def out = process.getText()
        logger.info("Request FE Config: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
    }

    def copy_prefix = """copy into ${tableName} from """;
    def properties = """properties ('file.type' = 'csv', 'file.column_separator' = ',', 'copy.async' = 'false', 'copy.use_delete_sign' = 'true')"""
    def deleteOnSql0 = copy_prefix + """ @~('${deleteFilePrefix}%d')""" + properties
    def deleteOnSql1 = copy_prefix + " (select from @~('${deleteFilePrefix}%d') )" + properties
    def deleteOnSql2 = copy_prefix + ' (select `$1`, $2, $3, $4 from' + """ @~('${deleteFilePrefix}%d') )""" + properties
    def deleteOnSql3 = copy_prefix + ' (select `$1`, $2, $3, $4 = 1 from' + """ @~('${deleteFilePrefix}%d') )""" + properties
    def deleteOnSql4 = copy_prefix + ' (select `$1`, $2, $3, $4 = 0 from' + """ @~('${deleteFilePrefix}%d') )""" + properties
    def deleteOnSql5 = copy_prefix + ' (select `$1`, $2, $4, $3 > 50 from' + """ @~('${deleteFilePrefix}%d') )""" + properties
    def deleteOnSql6 = copy_prefix + ' (select `$1`, $2, $3, $4 from' + """ @~('${deleteFilePrefix}%d') """ + ' where $3 > 40)' + properties
    def sqls = [deleteOnSql0, deleteOnSql1, deleteOnSql2, deleteOnSql3, deleteOnSql4, deleteOnSql5, deleteOnSql6]

    for (int i = 0; i < sqls.size(); i++) {
        for (int j = 0; j < 2; j++) {
            if (j == 0) {
                sql """ SET show_hidden_columns=false """
            } else {
                sql """ SET show_hidden_columns=true """
            }
            int index = j * sqls.size() + i
            def deleteSql = sqls[i]
            try {
                sql """ DROP TABLE IF EXISTS ${tableName}; """
                sql """
                CREATE TABLE IF NOT EXISTS ${tableName} (
                id INT,
                name varchar(20),
                score INT
                )
                UNIQUE KEY(id, name)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                """
                def fileName = "test_delete_on_0_" + index + ".csv"
                uploadFile(fileName, filePath);
                def result = sql """ 
                copy into ${tableName} from @~('${fileName}') properties 
                ('file.type' = 'csv', 'file.column_separator' = ',', 'copy.async' = 'false'); 
                """
                logger.info("copy result: " + result)
                assertTrue(result.size() == 1)
                assertTrue(result[0].size() == 8)
                assertTrue(result[0][1].equals("FINISHED"), "Finish copy into, state=" + result[0][1] + ", expected state=FINISHED")
                qt_sql " SELECT COUNT(*) FROM ${tableName}; "
                qt_sql "select * from ${tableName} order by id, name, score asc;"

                uploadFile(deleteFilePrefix + index, deleteFilePath);
                deleteSql = String.format(deleteSql, index)
                result = sql """ ${deleteSql}"""
                logger.info("copy result: " + result)
                assertTrue(result.size() == 1)
                assertTrue(result[0].size() == 8)
                assertTrue(result[0][1].equals("FINISHED"), "Finish copy into, state=" + result[0][1] + ", expected state=FINISHED")
                qt_sql " SELECT COUNT(*) FROM ${tableName}; "
                qt_sql "select * from ${tableName} order by id, name, score asc;"
            } finally {
                try_sql("DROP TABLE IF EXISTS ${tableName}")
            }
        }
    }

    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
        id INT,
        name varchar(20),
        score INT
        )
        UNIQUE KEY(id, name)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        """

        uploadFile("test_delete_on_error.csv", filePath)
        def deleteSql = copy_prefix + """ @~('test_delete_on_error.csv') """ + ' ' + properties
        def result = sql """ ${deleteSql}"""
        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        assertTrue(result[0][1].equals("CANCELLED"), "Finish copy into, state=" + result[0][1] + ", expected state=CANCELLED")
        assertTrue(result[0][2].equals("ETL_QUALITY_UNSATISFIED"))
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}