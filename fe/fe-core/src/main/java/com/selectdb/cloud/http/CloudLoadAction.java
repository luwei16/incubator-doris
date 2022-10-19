package com.selectdb.cloud.http;

import com.selectdb.cloud.proto.SelectdbCloud.StagePB;
import com.selectdb.cloud.proto.SelectdbCloud.StagePB.StageType;
import com.selectdb.cloud.storage.RemoteBase;
import com.selectdb.cloud.storage.RemoteBase.ObjectInfo;

import com.google.common.base.Strings;
import org.apache.doris.analysis.CopyStmt;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DorisHttpException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.rest.RestBaseController;
import org.apache.doris.httpv2.util.ExecutionResultSet;
import org.apache.doris.httpv2.util.HttpUtil;
import org.apache.doris.httpv2.util.StatementSubmitter;
import org.apache.doris.qe.ConnectContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
@RequestMapping(path = "/copy")
public class CloudLoadAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(CloudLoadAction.class);

    private static StatementSubmitter stmtSubmitter = new StatementSubmitter();

    // curl  -u user:password -H "fileName: file" -T file -L http://127.0.0.1:12104/copy/upload
    @RequestMapping(path = "/upload", method = RequestMethod.PUT)
    public Object copy(HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        Map<String, Object> resultMap = new HashMap<>(3);
        try {
            String fileName = request.getHeader("fileName");
            if (Strings.isNullOrEmpty(fileName)) {
                return ResponseEntityBuilder.badRequest("http header must have fileName entry");
            }
            String mysqlUserName = ClusterNamespace
                    .getNameFromFullName(ConnectContext.get().getCurrentUserIdentity().getQualifiedUser());
            LOG.info("receive Presigned url request [ user [{}]] for filename [{}]",
                    mysqlUserName, fileName);

            // use userName, fileName to get presigned url from ms EXTERNAL
            // 1. rpc to ms, by unique_id、username
            StagePB internalStage = Env.getCurrentInternalCatalog().getStage(StageType.INTERNAL,
                    mysqlUserName, fileName);
            // 2. call RemoteBase to get pre-signedUrl
            RemoteBase rb = RemoteBase.newInstance(new ObjectInfo(internalStage.getObjInfo()));
            String signedUrl = rb.getPresignedUrl(fileName);
            LOG.info("get internal stage remote info: {}, and signedUrl: {}", rb.toString(), signedUrl);
            return redirectToObj(signedUrl);
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

    @RequestMapping(path = "/query", method = RequestMethod.POST)
    public Object loadQuery(HttpServletRequest request, HttpServletResponse response) throws InterruptedException {
        ActionAuthorizationInfo authInfo = executeCheckPassword(request, response);
        String postContent = HttpUtil.getBody(request);
        Map<String, Object> resultMap = new HashMap<>(3);
        try {
            if (Strings.isNullOrEmpty(postContent)) {
                return ResponseEntityBuilder.badRequest("POST body must contain json object");
            }
            JSONObject jsonObject = (JSONObject) JSONValue.parse(postContent);
            if (jsonObject == null) {
                return ResponseEntityBuilder.badRequest("malformed json: " + postContent);
            }

            String copyIntoSql = (String) jsonObject.get("sql");

            if (Strings.isNullOrEmpty(copyIntoSql)) {
                return ResponseEntityBuilder.badRequest("POST body must contain [sql] root object");
            }

            StatementBase copyIntoStmt = StatementSubmitter.analyzeStmt(copyIntoSql);
            if (!(copyIntoStmt instanceof CopyStmt)) {
                return ResponseEntityBuilder.badRequest("just support copy into sql: " + copyIntoSql);
            }

            LOG.info("copy into stmt: {}", copyIntoSql);

            ConnectContext.get().changeDefaultCatalog(InternalCatalog.INTERNAL_CATALOG_NAME);

            return executeQuery(authInfo, copyIntoSql, response);
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

    /**
     * Execute a copy into
     * @param authInfo check user and password
     * @return response
     */
    private ResponseEntity executeQuery(ActionAuthorizationInfo authInfo,
                                        String copyIntoStmt, HttpServletResponse response) {
        StatementSubmitter.StmtContext stmtCtx = new StatementSubmitter.StmtContext(copyIntoStmt,
                authInfo.fullUserName, authInfo.password, 1000, false, response);
        Future<ExecutionResultSet> future = stmtSubmitter.submitBlock(stmtCtx);

        try {
            ExecutionResultSet resultSet = future.get();
            return ResponseEntityBuilder.ok(resultSet.getResult());
        } catch (InterruptedException e) {
            LOG.warn("failed to execute stmt {}, ", copyIntoStmt, e);
            return ResponseEntityBuilder.okWithCommonError("Failed to execute sql: " + e.getMessage());
        } catch (ExecutionException e) {
            LOG.warn("failed to execute stmt {}", copyIntoStmt, e);
            return ResponseEntityBuilder.okWithCommonError("Failed to execute sql: " + e.getMessage());
        }
    }
}
