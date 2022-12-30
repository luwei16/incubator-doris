import groovy.json.JsonOutput
import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_error_url") {
    def tableName = 'test_error_url'

    sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""
    sql """
        CREATE TABLE `${tableName}`
        (
            `siteid` INT DEFAULT '10',
            `citycode` SMALLINT,
            `username` VARCHAR(32) DEFAULT 'test',
            `pv` BIGINT SUM DEFAULT '0'
        )
        AGGREGATE KEY(`siteid`, `citycode`, `username`)
        DISTRIBUTED BY HASH(siteid) BUCKETS 1;
    """

    String label = UUID.randomUUID().toString()
    String errorUrl = null;
    streamLoad {
        table tableName

        // default label is UUID:
        set 'label', "${label}"

        // default column_separator is specify in doris fe config, usually is '\t'.
        // this line change to ','
        set 'column_separator', ','
        // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
        // also, you can stream load a http stream, e.g. http://xxx/some.csv
        file 'error_url_data.csv'

        time 10000 // limit inflight 10s

        // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

        // if declared a check callback, the default check condition will ignore.
        // So you must check all condition
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            errorUrl = json.ErrorURL;
        }
    }
    logger.info("ErrorUrl: ${errorUrl}");

    StringBuilder sb = new StringBuilder();
    sb.append("curl --verbose ")
    sb.append("${errorUrl}")
    String command = sb.toString()
    logger.info("command: ${command}");
    process = command.execute()
    code = process.waitFor()
    err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    out = process.getText()
    assertEquals(code, 0)
    logger.info("code:${code}, out: ${out}, err:${err}");
}
