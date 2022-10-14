import groovy.json.JsonOutput
suite("test_grant_revoke_cluster_to_user", "cloud_auth") {
    def token = "greedisgood9999"
    def instance_id = "instance_id_deadbeef_cluster"
    def name = "user_1"
    def user_id = "10000"

    // create instance
    /*
        curl -X GET '127.0.0.1:5000/MetaService/http/create_instance?token=greedisgood9999' -d '{
            "instance_id": "instance_id_deadbeef",
            "name": "user_1",
            "user_id": "10000",
            "obj_info": {
                "ak": "test-ak1",
                "sk": "test-sk1",
                "bucket": "test-bucket",
                "prefix": "test-prefix",
                "endpoint": "test-endpoint",
                "region": "test-region",
                "provider" : "BOS"
            }
        }'
     */

    def jsonOutput = new JsonOutput()
    def s3 = [ak: "test-ak1",
              sk : "test-sk1",
              bucket : "test-bucket",
              prefix: "test-prefix",
              endpoint: "test-endpoint",
              region: "test-region",
              provider : "BOS"]
    def map = [instance_id: "${instance_id}", name: "${name}", user_id: "${user_id}", obj_info: s3]
    def js = jsonOutput.toJson(map)

    def create_instance_api = { request_body, check_func ->
        httpTest {
            endpoint context.config.metaServiceHttpAddress
            uri "/MetaService/http/create_instance?token=$token"
            body request_body
            check check_func
        }
    }

    create_instance_api.call(js) {
        respCode, body ->
            log.info("http cli result: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("ALREADY_EXISTED"))
    }

    def clusterName = "gr_test_cluster_1"
    def clusterId = "gr_test_cluster_id1"
    def opType = "COMPUTE"
    def cloudUniqueId = "gr_cloud_unique_id_compute_node0"
    def ip = "172.0.0.50"
    def heartbeatPort = 9050
    def nodeMap = [cloud_unique_id: "${cloudUniqueId}", ip: "${ip}", heartbeat_port: "${heartbeatPort}"]
    def nodeList = [nodeMap]
    // add be cluster
    /*
        curl -X GET http://127.0.0.1:5000/MetaService/http/add_cluster?token=greedisgood9999 -d '{
            "instance_id": "instance_id_deadbeef",
            "cluster": {
                "cluster_name": "cluster_name1",
                "cluster_id": "cluster_id1",
                "type": "COMPUTE",
                "nodes": []
            }
        }'
     */
    def clusterMap = [cluster_name: "${clusterName}", cluster_id:"${clusterId}", type:"COMPUTE", nodes:nodeList]
    def instance = [instance_id: "${instance_id}", cluster: clusterMap]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)

    def add_cluster_api = { request_body, check_func ->
        httpTest {
            endpoint context.config.metaServiceHttpAddress
            uri "/MetaService/http/add_cluster?token=$token"
            body request_body
            check check_func
        }
    }

    add_cluster_api.call(js) {
        respCode, body ->
            log.info("http cli result: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("ALREADY_EXISTED"))
    }

    sleep(10000)

    def user1 = "regression_test_user1"
    def role = "admin"

    def fail1 = try_sql """
        GRANT USAGE_PRIV ON CLUSTER ${clusterName} TO ${user1};
    """
    // ERROR 1105 (HY000): errCode = 2, detailMessage = user 'default_cluster:user1'@'%' does not exist
    assertEquals(fail1, null)

    try_sql("DROP USER ${user1}")
    sql """CREATE USER '${user1}' IDENTIFIED BY '123456' DEFAULT ROLE '${role}'"""

    def fail2 = try_sql """
        GRANT USAGE_PRIV ON CLUSTER ${clusterName} TO ${user1};
    """
    // ERROR 1105 (HY000): errCode = 2, detailMessage = grant use cluster permissions failed, try later
    // nofity ms failed, because fe can't get regression's instance_id by fe's unique_id
    assertEquals(fail2, null)

    def result1 = connect(user=user1, password='123456', url=context.config.jdbcUrl) {
        def sg = try_sql """show grants"""
        assertEquals(sg.size(), 1)
        try_sql """use information_schema@${clusterName}"""
    }

    // ERROR 1105 (HY000): errCode = 2, detailMessage = grant use cluster permissions failed, try later
    // nofity ms failed, because fe can't get regression's instance_id by fe's unique_id
    def fail3 = try_sql """
        REVOKE USAGE_PRIV ON CLUSTER ${clusterName} FROM ${user1};
    """
    assertEquals(fail3, null)

    def succ4 = try_sql """
        DROP USER ${user1}
    """
    assertEquals(succ4.size(), 1)
}