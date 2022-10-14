package com.selectdb.cloud.storage;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.HttpMethod;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.GeneratePresignedUrlRequest;
import com.aliyun.oss.model.ListObjectsV2Request;
import com.aliyun.oss.model.ListObjectsV2Result;
import com.aliyun.oss.model.OSSObjectSummary;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.common.DdlException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class OssRemote extends DefaultRemote {
    private static final Logger LOG = LogManager.getLogger(OssRemote.class);
    private OSS ossClient;

    public OssRemote(ObjectInfo obj) {
        super(obj);
    }

    @Override
    public String getPresignedUrl(String fileName) {
        String bucketName = obj.getBucket();
        String objectName = obj.getPrefix() + fileName;
        initClient();
        try {
            int count = 0;
            while (true) {
                GeneratePresignedUrlRequest request
                        = new GeneratePresignedUrlRequest(bucketName, objectName, HttpMethod.PUT);
                Date expiration = new Date(new Date().getTime() + 3600 * 1000 + count * 500L);
                request.setExpiration(expiration);
                URL signedUrl = ossClient.generatePresignedUrl(request);
                LOG.debug("before change url: {}, count: {}", signedUrl.toString(), count);
                // fix oss bug, curl redirect deal "%2B" has bug.
                String sign = signedUrl.toString();
                if (sign.contains("%2B")) {
                    ++count;
                    continue;
                }
                return signedUrl.toString();
            }
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
            close();
        }
        return "";
    }

    @Override
    public ListObjectsResult listObjects(String continuationToken) throws DdlException {
        initClient();
        try {
            ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(obj.getBucket())
                    .withPrefix(normalizePrefix());
            if (!StringUtils.isEmpty(continuationToken)) {
                request.setContinuationToken(continuationToken);
            }
            ListObjectsV2Result result = ossClient.listObjectsV2(request);
            List<ObjectFile> objectFiles = new ArrayList<>();
            for (OSSObjectSummary s : result.getObjectSummaries()) {
                objectFiles.add(new ObjectFile(s.getKey(), getRelativePath(s.getKey()), s.getETag(), s.getSize()));
            }
            return new ListObjectsResult(objectFiles, result.isTruncated(), request.getContinuationToken());
        } catch (OSSException e) {
            LOG.warn("Failed to list objects for OSS", e);
            throw new DdlException("Failed to list objects for OSS, Error code=" + e.getErrorCode() + ", Error message="
                    + e.getErrorMessage());
        }
    }

    private void initClient() {
        if (ossClient == null) {
            ossClient = new OSSClientBuilder().build(obj.getEndpoint(), obj.getAk(), obj.getSk());
        }
    }

    @Override
    public void close() {
        if (ossClient != null) {
            ossClient.shutdown();
            ossClient = null;
        }
    }

    @Override
    public String toString() {
        return "OssRemote{"
            + "obj=" + obj
            + '}';
    }
}
