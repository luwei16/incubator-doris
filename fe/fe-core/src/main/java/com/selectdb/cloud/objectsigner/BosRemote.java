package com.selectdb.cloud.objectsigner;

import com.baidubce.auth.DefaultBceCredentials;
import com.baidubce.http.HttpMethodName;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosClientConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URL;

public class BosRemote extends RemoteBase {
    private static final Logger LOG = LogManager.getLogger(BosRemote.class);

    public BosRemote(ObjectInfo obj) {
        super(obj);
    }

    public String getPresignedUrl(String fileName) {
        BosClientConfiguration config = new BosClientConfiguration();
        config.setCredentials(new DefaultBceCredentials(obj.getAk(), obj.getSk()));
        config.setEndpoint(obj.getEndpoint());
        BosClient client = new BosClient(config);
        URL url = client.generatePresignedUrl(obj.getBucket(), obj.getPrefix() + fileName, 3600, HttpMethodName.PUT);
        LOG.info("Bos getPresignedUrl: {}", url);
        return url.toString();
    }

    @Override
    public String toString() {
        return "BosRemote{"
            + "obj=" + obj
            + '}';
    }
}
