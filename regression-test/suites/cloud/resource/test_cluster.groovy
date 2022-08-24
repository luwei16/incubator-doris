import groovy.json.JsonOutput
import org.codehaus.groovy.runtime.IOGroovyMethods
suite("cloud_cluster_test", "cloud_cluster") {
    def getMsUrls = { api, token, jsonStr ->
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X GET http://")
        sb.append(context.config.metaServiceHttpAddress)
        sb.append("/MetaService/http/$api?token=")
        sb.append(token)
        sb.append(" -d ")
        sb.append(jsonStr)
        return sb.toString()
    }

    def token = "greedisgood9999"
    def instance_id = "instance_id_deadbeef"
    def name = "user_1"
    def user_id = "10000"

    // create instance
    // curl -X GET '127.0.0.1:5000/MetaService/http/create_instance?token=greedisgood9999' -d '{"instance_id":"dx_dnstance_id_deadbeef_1","name":"dengxin","user_id":"999999"}'
    def jsonOutput = new JsonOutput()
    def map = [instance_id: "${instance_id}", name: "${name}", user_id: "${user_id}"]
    def js = jsonOutput.toJson(map)
    
    String command = getMsUrls.call("create_instance", token, js)
    logger.info("create instance url : " + command.toString())
    def process = command.execute()
    int code = process.waitFor()
    String err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    String out = process.getText()
    logger.info("Run create instance : code=" + code + ", out=" + out + ", err=" + err)
    def s1 = """instance already existed, instance_id=${instance_id}"""
    assertTrue(out.equalsIgnoreCase("OK") || out.startsWith(s1))
    def clusterName = "cluster_name1"
    def clusterId = "cluster_id1"
    def opType = 1
    def cloudUniqueId = "cloud_unique_id_compute_node0"
    def ip = "172.0.0.10"
    def heartbeatPort = 9050
    def nodeMap = [cloud_unique_id: "${cloudUniqueId}", ip: "${ip}", heartbeat_port: "${heartbeatPort}"]
    def nodeList = [nodeMap]
    def clusterMap = [cluster_name: "${clusterName}", cluster_id:"${clusterId}", nodes:nodeList]
    def instance = [instance_id: "${instance_id}", op:"${opType}", cluster: clusterMap]

    
    // add no be cluster
    // curl -X GET http://127.0.0.1:5000/MetaService/http/add_cluster?token=greedisgood9999 -d '{"instance_id":"instance_id_deadbeef","op":"1","cluster":{"cluster_name":"cluster_name1","cluster_id":"cluster_id1","nodes":[]}}'
    nodeList = []
    clusterMap = [cluster_name: "${clusterName}", cluster_id:"${clusterId}", nodes:nodeList]
    instance = [instance_id: "${instance_id}", op:"${opType}", cluster: clusterMap]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)
    command = getMsUrls.call("add_cluster", token, js)
    logger.info("add cluster url : " + command.toString())
    process = command.execute()
    code = process.waitFor()
    err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    out = process.getText()
    logger.info("Run add cluster : code=" + code + ", out=" + out + ", err=" + err)
    assertTrue(out.startsWith("OK") || out.startsWith("try to add a existing cluster"))

    opType = 2
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)

    command = getMsUrls.call("drop_cluster", token, js)
    logger.info("drop cluster no be url : " + command.toString())
    process = command.execute()
    code = process.waitFor()
    err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    out = process.getText()
    logger.info("Run drop cluster : code=" + code + ", out=" + out + ", err=" + err)
    assertTrue(out.startsWith("OK") || out.startsWith("cloud_unique_id is already occupied by an instance"))
    nodeMap = [cloud_unique_id: "${cloudUniqueId}", ip: "${ip}", heartbeat_port: "${heartbeatPort}"]
    nodeList = [nodeMap]
    clusterMap = [cluster_name: "${clusterName}", cluster_id:"${clusterId}", nodes:nodeList]
    instance = [instance_id: "${instance_id}", op:"${opType}", cluster: clusterMap]

    // add_cluster
    // curl '127.0.0.1:5000/MetaService/http/add_cluster?token=greedisgood9999' -d '{
    //     "instance_id":"instance_id_deadbeef",
    //     "type":1,
    //     "cluster":{
    //         "cluster_name":"cluster_name1",
    //         "cluster_id":"cluster_id1",
    //         "nodes":[
    //             {
    //                 "cloud_unique_id":"cloud_unique_id_compute_node0",
    //                 "ip":"172.0.0.10",
    //                 "heartbeat_port":9050
    //             }
    //         ]
    //     }
    // }'
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)

    command = getMsUrls.call("add_cluster", token, js)
    logger.info("add cluster url : " + command.toString())
    process = command.execute()
    code = process.waitFor()
    err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    out = process.getText()
    logger.info("Run add cluster : code=" + code + ", out=" + out + ", err=" + err)
    assertTrue(out.startsWith("OK") || out.startsWith("cloud_unique_id is already occupied by an instance"))

    // add again, failed
    command = getMsUrls.call("add_cluster", token, js)
    logger.info("add cluster again url : " + command.toString())
    process = command.execute()
    code = process.waitFor()
    err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    out = process.getText()
    logger.info("Run add cluster : code=" + code + ", out=" + out + ", err=" + err)
    assertTrue(out.startsWith("cloud_unique_id is already occupied by an instance"))

    // change instance_id add same cloud_unique_id node failed
    // err: cloud_unique_id is already occupied by an instance, instance_id=instance_id_deadbeef cluster_name=cluster_name1 cluster_id=cluster_id1 cloud_unique_id=cloud_unique_id_compute_node0
    instance_id = "instance_id_deadbeef_1"
    nodeMap = [cloud_unique_id: "${cloudUniqueId}", ip: "${ip}", heartbeat_port: "${heartbeatPort}"]
    nodeList = [nodeMap]
    clusterMap = [cluster_name: "${clusterName}", cluster_id:"${clusterId}", nodes:nodeList]
    instance = [instance_id: "${instance_id}", op:"${opType}", cluster: clusterMap]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)

    command = getMsUrls.call("add_cluster", token, js)
    logger.info("add cluster url : " + command.toString())
    process = command.execute()
    code = process.waitFor()
    err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    out = process.getText()
    logger.info("Run add cluster : code=" + code + ", out=" + out + ", err=" + err)
    assertTrue(out.startsWith("cloud_unique_id is already occupied by an instance"))


    // get_cluster by cluster name
    // curl '127.0.0.1:5000/MetaService/http/get_cluster?token=greedisgood9999' -d '{"cloud_unique_id": "cloud_unique_id_compute_node0", "instance_id":"instance_id_deadbeef", "cluster_name":"cluster_name0"}'
    instance_id = "instance_id_deadbeef"
    instance = [cloud_unique_id: "${cloudUniqueId}", instance_id: "${instance_id}", cluster_name: "${clusterName}"]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)

    command = getMsUrls.call("get_cluster", token, js)
    logger.info("get cluster url by name : " + command.toString())
    process = command.execute()
    code = process.waitFor()
    err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    out = process.getText()
    logger.info("Run get cluster : code=" + code + ", out=" + out + ", err=" + err)
    assertTrue(out.startsWith("{\"cluster_id\"") || out.startsWith("fail to get cluster with instance_id"))
    

    // get_cluster by cluster id
    // curl '127.0.0.1:5000/MetaService/http/get_cluster?token=greedisgood9999' -d '{"cloud_unique_id": "cloud_unique_id_compute_node0", "instance_id":"instance_id_deadbeef", "cluster_id":"cluster_id1"}'
    instance = [cloud_unique_id: "${cloudUniqueId}", instance_id: "${instance_id}", cluster_id: "${clusterId}"]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)

    command = getMsUrls.call("get_cluster", token, js)
    logger.info("get cluster url by id : " + command.toString())
    process = command.execute()
    code = process.waitFor()
    err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    out = process.getText()
    logger.info("Run get cluster : code=" + code + ", out=" + out + ", err=" + err)
    assertTrue(out.startsWith("{\"cluster_id\"") || out.startsWith("fail to get cluster with instance_id"))
    

    // drop cluster    
    // curl '127.0.0.1:5000/MetaService/http/drop_cluster?token=greedisgood9999' -d '{
    //     "instance_id":"instance_id_deadbeef",
    //     "op":2,
    //     "cluster":{
    //         "cluster_name":"cluster_name1",
    //         "cluster_id":"cluster_id1",
    //         "nodes":[
    //             {
    //                 "cloud_unique_id":"cloud_unique_id_compute_node0",
    //                 "ip":"127.0.0.1",
    //                 "heartbeat_port":9050
    //             }
    //         ]
    //     }
    // }'

    opType = 2
    nodeMap = [cloud_unique_id: "${cloudUniqueId}", ip: "${ip}", heartbeat_port: "${heartbeatPort}"]
    nodeList = [nodeMap]
    clusterMap = [cluster_name: "${clusterName}", cluster_id:"${clusterId}", nodes:nodeList]
    instance = [instance_id: "${instance_id}", op:"${opType}", cluster: clusterMap]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)

    command = getMsUrls.call("drop_cluster", token, js)
    logger.info("drop cluster url : " + command.toString())
    process = command.execute()
    code = process.waitFor()
    err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    out = process.getText()
    logger.info("Run drop cluster : code=" + code + ", out=" + out + ", err=" + err)
    // ATTN: The multiple conditions here are to prevent abnormal exit, kV data is not cleaned up, and regression cases are always abnormal
    assertTrue(out.startsWith("OK") || out.startsWith("cloud_unique_id is already occupied by an instance") || out.startsWith("failed to find cluster to drop"))

    // drop again failed
    command = getMsUrls.call("drop_cluster", token, js)
    logger.info("drop cluster again url : " + command.toString())
    process = command.execute()
    code = process.waitFor()
    err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    out = process.getText()
    logger.info("Run drop cluster : code=" + code + ", out=" + out + ", err=" + err)
    assertTrue(out.startsWith("failed to find cluster to drop"))

    // drop not exist cluster, falied
    clusterMap = [cluster_name: "not_exist_cluster_name", cluster_id:"not_exist_cluster_id", nodes:nodeList]
    instance = [instance_id: "${instance_id}", op:"${opType}", cluster: clusterMap]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)
    command = getMsUrls.call("drop_cluster", token, js)
    logger.info("drop cluster again url : " + command.toString())
    process = command.execute()
    code = process.waitFor()
    err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    out = process.getText()
    logger.info("Run drop cluster : code=" + code + ", out=" + out + ", err=" + err)
    assertTrue(out.startsWith("failed to find cluster to drop"))
    clusterMap = [cluster_name: "${clusterName}", cluster_id:"${clusterId}", nodes:nodeList]
    instance = [instance_id: "${instance_id}", op:"${opType}", cluster: clusterMap]

    // after drop, get cluster again, failed
    instance = [cloud_unique_id: "${cloudUniqueId}", instance_id: "${instance_id}", cluster_id: "${clusterId}"]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)

    command = getMsUrls.call("get_cluster", token, js)
    logger.info("get cluster url by id : " + command.toString())
    process = command.execute()
    code = process.waitFor()
    err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    out = process.getText()
    logger.info("Run get cluster : code=" + code + ", out=" + out + ", err=" + err)
    assertTrue(out.startsWith("fail to get cluster with instance_id"))

    // add node to another cluster
    clusterName = "cluster_name2"
    clusterId = "cluster_id2"
    nodeMap = [cloud_unique_id: "${cloudUniqueId}", ip: "${ip}", heartbeat_port: "${heartbeatPort}"]
    nodeList = [nodeMap]
    clusterMap = [cluster_name: "${clusterName}", cluster_id:"${clusterId}", nodes:nodeList]
    instance = [instance_id: "${instance_id}", op:"${opType}", cluster: clusterMap]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)

    command = getMsUrls.call("add_cluster", token, js)
    logger.info("add to another cluster url : " + command.toString())
    process = command.execute()
    code = process.waitFor()
    err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    out = process.getText()
    logger.info("Run add cluster : code=" + code + ", out=" + out + ", err=" + err)
    assertTrue(out.startsWith("OK") || out.startsWith("cloud_unique_id is already occupied by an instance"))

    opType = 2
    instance = [instance_id: "${instance_id}", op:"${opType}", cluster: clusterMap]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)
    command = getMsUrls.call("drop_cluster", token, js)
    logger.info("drop to another cluster url : " + command.toString())
    process = command.execute()
    code = process.waitFor()
    err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    out = process.getText()
    logger.info("Run drop cluster : code=" + code + ", out=" + out + ", err=" + err)
    assertTrue(out.startsWith("OK") || out.startsWith("cloud_unique_id is already occupied by an instance"))
}