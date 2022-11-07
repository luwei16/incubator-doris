package com.selectdb.cloud.storage;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.http.HttpMethodName;
import com.qcloud.cos.http.HttpProtocol;
import com.qcloud.cos.region.Region;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URL;
import java.util.Date;
import java.util.HashMap;

public class CosRemote extends DefaultRemote {
    private static final Logger LOG = LogManager.getLogger(CosRemote.class);

    public CosRemote(ObjectInfo obj) {
        super(obj);
    }

    @Override
    public String getPresignedUrl(String fileName) {
        COSCredentials cred = new BasicCOSCredentials(obj.getAk(), obj.getSk());
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setRegion(new Region(obj.getRegion()));
        clientConfig.setHttpProtocol(HttpProtocol.https);
        COSClient cosClient = new COSClient(cred, clientConfig);
        Date expirationDate = new Date(System.currentTimeMillis() + 60 * 60 * 1000);
        URL url = cosClient.generatePresignedUrl(obj.getBucket(),
                normalizePrefix(fileName), expirationDate, HttpMethodName.PUT,
                new HashMap<String, String>(), new HashMap<String, String>());
        cosClient.shutdown();
        LOG.info("generate cos presigned url: {}", url);
        return url.toString();
    }

    @Override
    public String toString() {
        return "CosRemote{"
            + "obj=" + obj
            + '}';
    }
}
