package com.selectdb.cloud.http;

import com.selectdb.cloud.objectsigner.RemoteBase;
import com.selectdb.cloud.objectsigner.RemoteBase.ObjectInfo;
import com.selectdb.cloud.proto.SelectdbCloud.StagePB;
import com.selectdb.cloud.proto.SelectdbCloud.StagePB.StageType;

import com.google.common.base.Strings;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DorisHttpException;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.rest.RestBaseController;
import org.apache.doris.httpv2.util.HttpUtil;
import org.apache.doris.qe.ConnectContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
public class CloudLoadAction extends RestBaseController {

    private static final Logger LOG = LogManager.getLogger(CloudLoadAction.class);

    @RequestMapping(path = "/load/presigned_url", method = RequestMethod.POST)
    public Object loadPresignedUrl(HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        String postContent = HttpUtil.getBody(request);
        Map<String, Object> resultMap = new HashMap<>(3);
        try {
            if (Strings.isNullOrEmpty(postContent)) {
                return ResponseEntityBuilder.badRequest("POST body must contains json object");
            }
            JSONObject jsonObject = (JSONObject) JSONValue.parse(postContent);
            if (jsonObject == null) {
                return ResponseEntityBuilder.badRequest("malformed json: " + postContent);
            }

            String filename = (String) jsonObject.get("filename");

            if (Strings.isNullOrEmpty(filename)) {
                return ResponseEntityBuilder.badRequest("POST body must contains [filename] root object");
            }
            String mysqlUserName = ClusterNamespace
                    .getNameFromFullName(ConnectContext.get().getCurrentUserIdentity().getQualifiedUser());
            LOG.info("receive Presigned url request [ user [{}]] for filename [{}]",
                    mysqlUserName, filename);

            // use userName, fileName to get presigned url from ms EXTERNAL
            // 1. rpc to ms, by unique_id、username
            StagePB internalStage = Env.getCurrentInternalCatalog().getStage(StageType.INTERNAL,
                    mysqlUserName, filename);
            // 2. call RemoteBase to get pre-signedUrl
            RemoteBase rb = RemoteBase.newInstance(new ObjectInfo(internalStage.getObjInfo().getProvider(),
                    internalStage.getObjInfo().getAk(), internalStage.getObjInfo().getSk(),
                    internalStage.getObjInfo().getBucket(), internalStage.getObjInfo().getEndpoint(),
                    internalStage.getObjInfo().getRegion(), internalStage.getObjInfo().getPrefix()));
            LOG.debug("get internal stage remote info: {}", rb.toString());
            String signedUrl = rb.getPresignedUrl(filename);
            resultMap.put("url", signedUrl);
        } catch (DorisHttpException e) {
            // status code  should conforms to HTTP semantic
            resultMap.put("code", e.getCode().code());
            resultMap.put("msg", e.getMessage());
        } catch (Exception e) {
            resultMap.put("code", "1");
            resultMap.put("exception", e.getMessage());
        }
        return ResponseEntityBuilder.ok(resultMap);
    }
}
