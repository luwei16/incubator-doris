package com.selectdb.cloud.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

import java.net.URL;
import java.time.Duration;

public class S3Remote extends DefaultRemote {
    private static final Logger LOG = LogManager.getLogger(S3Remote.class);

    public S3Remote(ObjectInfo obj) {
        super(obj);
    }

    @Override
    public String getPresignedUrl(String fileName) {
        try {
            PutObjectRequest objectRequest = PutObjectRequest.builder()
                    .bucket(obj.getBucket())
                    .key(obj.getPrefix() + fileName)
                    .build();

            PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofMinutes(60))
                    .putObjectRequest(objectRequest)
                    .build();

            AwsBasicCredentials credentialsProvider = AwsBasicCredentials.create(obj.getAk(), obj.getSk());
            Region region = Region.of(obj.getRegion());
            S3Presigner presigner = S3Presigner.builder()
                    .region(region)
                    .credentialsProvider(StaticCredentialsProvider.create(credentialsProvider))
                    .build();

            PresignedPutObjectRequest presignedRequest = presigner.presignPutObject(presignRequest);
            String presignedURL = presignedRequest.url().toString();
            LOG.info("Presigned URL to upload a file to: {}", presignedURL);

            // Upload content to the Amazon S3 bucket by using this URL.
            URL url = presignedRequest.url();
            return url.toString();

        } catch (S3Exception e) {
            e.getStackTrace();
        }
        return "";
    }

    @Override
    public String toString() {
        return "S3Remote{"
            + "obj=" + obj
            + '}';
    }
}
