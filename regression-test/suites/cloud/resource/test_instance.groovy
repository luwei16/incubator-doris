import groovy.json.JsonOutput
suite("cloud_instance_test", "cloud_instance") {
    def token = "greedisgood9999"
    def instance_id = "instance_id_deadbeef"
    def name = "user_1"
    def user_id = "10000"
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
                "region": "test-region"
            }
        }'
     */

    def jsonOutput = new JsonOutput()
    def s3 = [ak: "test-ak1",
              sk : "test-sk1"
              ,bucket : "test-bucket", prefix: "test-prefix", endpoint: "test-endpoint"
              ,region: "test-region"]
    def map = [instance_id: "instance_id", name: "${name}", user_id: "${user_id}", obj_info: s3]
    def js = jsonOutput.toJson(map)

    def create_instance_api = { request_body, check_func ->
        httpTest {
            uri "/MetaService/http/create_instance?token=greedisgood9999"
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

    // create again failed
    create_instance_api.call(js) {
        respCode, body ->
            log.info("http cli result: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("ALREADY_EXISTED"))
    }
}
