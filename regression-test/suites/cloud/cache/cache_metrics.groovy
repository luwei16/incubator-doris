import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_cache_metrics") {
    String[][] backends = sql """ show backends; """
    assertTrue(backends.size() > 0)
    String backend_id;
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    for (String[] backend in backends) {
        if (backend[9].equals("true")) {
            backendId_to_backendIP.put(backend[0], backend[2])
            backendId_to_backendHttpPort.put(backend[0], backend[5])
        }
    }

    backend_id = backendId_to_backendIP.keySet()[0]
    StringBuilder showConfigCommand = new StringBuilder();
    showConfigCommand.append("curl -X GET http://")
    showConfigCommand.append(backendId_to_backendIP.get(backend_id))
    showConfigCommand.append(":")
    showConfigCommand.append(backendId_to_backendHttpPort.get(backend_id))
    showConfigCommand.append("/metrics")
    logger.info(showConfigCommand.toString())
    def process = showConfigCommand.toString().execute()
    int code = process.waitFor()
    String err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    String out = process.getText()
    assertEquals(code, 0)
    def strs = out.split('\n')
    Boolean flag = false;
    for (String line in strs) {
        if (line.startsWith("doris_be_cache")) {
            flag = true;
            break;
        }
    }
    assertTrue(flag);
}
