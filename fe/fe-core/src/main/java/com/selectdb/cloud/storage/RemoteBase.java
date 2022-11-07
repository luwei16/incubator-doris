package com.selectdb.cloud.storage;

import com.selectdb.cloud.proto.SelectdbCloud;

import lombok.Getter;
import org.apache.doris.common.DdlException;

public abstract class RemoteBase {
    @Getter
    public static class ObjectInfo {
        private final SelectdbCloud.ObjectStoreInfoPB.Provider provider;
        private final String ak;
        private final String sk;
        private final String bucket;
        private final String endpoint;
        private final String region;
        private final String prefix;

        public ObjectInfo(SelectdbCloud.ObjectStoreInfoPB.Provider provider,
                          String ak, String sk, String bucket, String endpoint, String region, String prefix) {
            this.provider = provider;
            this.ak = ak;
            this.sk = sk;
            this.bucket = bucket;
            this.endpoint = endpoint;
            this.region = region;
            this.prefix = prefix;
        }

        public ObjectInfo(SelectdbCloud.ObjectStoreInfoPB objectStoreInfoPB) {
            this.provider = objectStoreInfoPB.getProvider();
            this.ak = objectStoreInfoPB.getAk();
            this.sk = objectStoreInfoPB.getSk();
            this.bucket = objectStoreInfoPB.getBucket();
            this.endpoint = objectStoreInfoPB.getEndpoint();
            this.region = objectStoreInfoPB.getRegion();
            this.prefix = objectStoreInfoPB.getPrefix();
        }

        @Override
        public String toString() {
            return "Obj{"
                + "provider=" + provider
                + ", ak='" + ak + '\''
                + ", sk='" + sk + '\''
                + ", bucket='" + bucket + '\''
                + ", endpoint='" + endpoint + '\''
                + ", region='" + region + '\''
                + ", prefix='" + prefix + '\''
                + '}';
        }
    }

    public ObjectInfo obj;

    public RemoteBase(ObjectInfo obj) {
        this.obj = obj;
    }

    public String getPresignedUrl(String fileName) {
        return "not impl";
    }

    public abstract ListObjectsResult listObjects(String continuationToken) throws DdlException;

    public abstract ListObjectsResult listObjects(String subPrefix, String continuationToken) throws DdlException;

    public abstract ListObjectsResult headObject(String subKey) throws DdlException;

    public void close() {}

    public static RemoteBase newInstance(ObjectInfo obj) throws Exception {
        switch (obj.provider) {
            case OSS:
                return new OssRemote(obj);
            case S3:
                return new S3Remote(obj);
            case COS:
                return new CosRemote(obj);
            case OBS:
                return new ObsRemote(obj);
            case BOS:
                return new BosRemote(obj);
            default:
                throw new Exception("current not support obj : " + obj.toString());
        }
    }

    protected String normalizePrefix() {
        return obj.prefix.isEmpty() ? "" : (obj.prefix.endsWith("/") ? obj.prefix : String.format("%s/", obj.prefix));
    }

    protected String normalizePrefix(String subPrefix) {
        String prefix = normalizePrefix();
        // if prefix is not empty, prefix contains '/' in the end
        return prefix.isEmpty() ? subPrefix : String.format("%s%s", prefix, subPrefix);
    }

    protected String getRelativePath(String key) throws DdlException {
        String expectedPrefix = normalizePrefix();
        if (!key.startsWith(expectedPrefix)) {
            throw new DdlException(
                    "List a object whose key: " + key + " does not start with object prefix: " + expectedPrefix);
        }
        return key.substring(expectedPrefix.length());
    }

    // The etag returned by S3 SDK contains quota in the head and tail, such as "9de7058b7d5816b72d90544810740c1c"
    // The etag returned by OSS SDK does not contain quota, such as 9de7058b7d5816b72d90544810740c1c
    // So add quota for etag returned by OSS SDK.
    protected String formatEtag(String etag) {
        if (!etag.startsWith("\"") && !etag.endsWith("\"")) {
            return String.format("\"%s\"", etag);
        }
        if (etag.startsWith("\"") && etag.endsWith("\"")) {
            return etag;
        }
        throw new IllegalArgumentException("Invalid etag=" + etag);
    }
}
