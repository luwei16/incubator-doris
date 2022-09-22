package com.selectdb.cloud.objectsigner;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.HttpMethod;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.GeneratePresignedUrlRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URL;
import java.util.Date;

public class OssRemote extends RemoteBase {
    private static final Logger LOG = LogManager.getLogger(OssRemote.class);

    public OssRemote(ObjectInfo obj) {
        super(obj);
    }

    public String getPresignedUrl(String fileName) {
        String endpoint = obj.getEndpoint();
        String accessKeyId = obj.getAk();
        String accessKeySecret = obj.getSk();
        String bucketName = obj.getBucket();
        String objectName = obj.getPrefix() + fileName;
        OSS ossClient = new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret);
        try {
            GeneratePresignedUrlRequest request
                    = new GeneratePresignedUrlRequest(bucketName, objectName, HttpMethod.PUT);
            Date expiration = new Date(new Date().getTime() + 3600 * 1000);
            request.setExpiration(expiration);
            URL signedUrl = ossClient.generatePresignedUrl(request);
            return signedUrl.toString();
        } catch (OSSException oe) {
            LOG.warn("Caught an OSSException, which means your request made it to OSS, "
                    + "but was rejected with an error response for some reason. "
                    + "Error Message: {} , Error Code: {} Request ID: {} Host ID: {}",
                    oe.getErrorMessage(), oe.getErrorCode(), oe.getRequestId(), oe.getHostId());
        } catch (ClientException ce) {
            LOG.warn("Caught an ClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with OSS, "
                    + "such as not being able to access the network. Error Message: {}", ce.getMessage());
        } finally {
            if (ossClient != null) {
                ossClient.shutdown();
            }
        }
        return "";
    }

    @Override
    public String toString() {
        return "OssRemote{"
            + "obj=" + obj
            + '}';
    }
}
