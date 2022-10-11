package com.selectdb.cloud.storage;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.common.DdlException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Default implementation of {@link RemoteBase} use {@link S3Client}.
 * If one object storage such as OSS can not use {@link S3Client} to access, please override these methods.
 */
public class DefaultRemote extends RemoteBase {
    private static final Logger LOG = LogManager.getLogger(DefaultRemote.class);
    private S3Client s3Client;

    public DefaultRemote(ObjectInfo obj) {
        super(obj);
    }

    @Override
    public ListObjectsResult listObjects(String continuationToken) throws DdlException {
        if (s3Client == null) {
            AwsBasicCredentials credentials = AwsBasicCredentials.create(obj.getAk(), obj.getSk());
            StaticCredentialsProvider scp = StaticCredentialsProvider.create(credentials);
            URI endpointUri = URI.create("http://" + obj.getEndpoint());
            s3Client = S3Client.builder().endpointOverride(endpointUri).credentialsProvider(scp)
                    .region(Region.of(obj.getRegion())).build();
        }
        try {
            ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder().bucket(obj.getBucket())
                    .prefix(normalizePrefix());
            if (!StringUtils.isEmpty(continuationToken)) {
                requestBuilder.continuationToken(continuationToken);
            }
            ListObjectsV2Response response = s3Client.listObjectsV2(requestBuilder.build());
            List<ObjectFile> objectFiles = new ArrayList<>();
            for (S3Object c : response.contents()) {
                objectFiles.add(new ObjectFile(c.key(), getRelativePath(c.key()), c.eTag(), c.size()));
            }
            return new ListObjectsResult(objectFiles, response.isTruncated(), response.nextContinuationToken());
        } catch (SdkException e) {
            LOG.warn("Failed to list objects for S3", e);
            throw new DdlException("Failed to list objects for S3, Error message=" + e.getMessage());
        }
    }

    @Override
    public void close() {
        if (s3Client != null) {
            s3Client.close();
            s3Client = null;
        }
    }
}
