import groovy.json.JsonOutput
import org.codehaus.groovy.runtime.IOGroovyMethods
suite("cloud_instance_test", "cloud_instance") {
    def token = "greedisgood9999"
    def instance_id = "instance_id_deadbeef"
    def name = "user_1"
    def user_id = "10000"
    // curl -X GET '127.0.0.1:5000/MetaService/http/create_instance?token=greedisgood9999' -d '{"instance_id":"dx_dnstance_id_deadbeef_1","name":"dengxin","user_id":"999999"}'
    StringBuilder sb = new StringBuilder();
    sb.append("curl -X GET http://")
    sb.append(context.config.metaServiceHttpAddress)
    sb.append("/MetaService/http/create_instance?token=")
    sb.append("${token}")
    sb.append(" -d ")

    def jsonOutput = new JsonOutput()
    def map = [instance_id: "${instance_id}", name: "${name}", user_id: "${user_id}"]
    def js = jsonOutput.toJson(map)
    sb.append(js)
    String command = sb.toString()
    logger.info("create instance url : " + command.toString())
    def process = command.execute()
    int code = process.waitFor()
    String err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    String out = process.getText()
    logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)

    def s1 = """instance already existed, instance_id=${instance_id}"""
    assertTrue(out.equalsIgnoreCase("OK") || out.startsWith(s1))

    // create again failed
    logger.info("create instance again url : " + command.toString())
    process = command.execute()
    code = process.waitFor()
    err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    out = process.getText()
    logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
    assertTrue(out.startsWith(s1))
}