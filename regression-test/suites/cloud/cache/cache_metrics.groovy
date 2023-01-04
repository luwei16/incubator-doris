import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_cache_metrics") {
    String[][] backends = sql """ show backends; """
    assertTrue(backends.size() > 0)
    String backendId;
    def backendIdToBackendIP = [:]
    def backendIdToBackendHttpPort = [:]
    for (String[] backend in backends) {
        if (backend[9].equals("true")) {
            backendIdToBackendIP.put(backend[0], backend[2])
            backendIdToBackendHttpPort.put(backend[0], backend[5])
        }
    }

    backendId = backendIdToBackendIP.keySet()[0]
    def getMetricsMethod = { check_func ->
        httpTest {
            endpoint backendIdToBackendIP.get(backendId) + ":" + backendIdToBackendHttpPort.get(backendId)
            uri "/metrics"
            op "get"
            check check_func
        }
    }

    getMetricsMethod.call() {
        respCode, body ->
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
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
}
