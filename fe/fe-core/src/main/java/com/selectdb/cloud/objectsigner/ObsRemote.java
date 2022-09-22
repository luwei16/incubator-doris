package com.selectdb.cloud.objectsigner;

import com.obs.services.ObsClient;
import com.obs.services.model.HttpMethodEnum;
import com.obs.services.model.TemporarySignatureRequest;
import com.obs.services.model.TemporarySignatureResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;

public class ObsRemote extends RemoteBase {
    private static final Logger LOG = LogManager.getLogger(ObsRemote.class);

    public ObsRemote(ObjectInfo obj) {
        super(obj);
    }

    public String getPresignedUrl(String fileName) {
        String endPoint = obj.getEndpoint();
        String ak = obj.getAk();
        String sk = obj.getSk();

        ObsClient obsClient = new ObsClient(ak, sk, endPoint);
        long expireSeconds = 3600L;
        TemporarySignatureRequest request = new TemporarySignatureRequest(HttpMethodEnum.PUT, expireSeconds);
        request.setBucketName(obj.getBucket());
        request.setObjectKey(obj.getPrefix() + fileName);
        request.setHeaders(new HashMap<String, String>());

        TemporarySignatureResponse response = obsClient.createTemporarySignature(request);

        String url = response.getSignedUrl();
        LOG.info("obs temporary signature url: {}", url);
        return url;
    }

    @Override
    public String toString() {
        return "ObsRemote{"
            + "obj=" + obj
            + '}';
    }
}
