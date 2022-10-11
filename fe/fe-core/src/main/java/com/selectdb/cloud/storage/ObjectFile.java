package com.selectdb.cloud.storage;

import lombok.Getter;

public class ObjectFile {
    @Getter
    private String key;
    @Getter
    private String relativePath;
    @Getter
    private String etag;
    @Getter
    private long size;

    public ObjectFile(String key, String relativePath, String etag, long size) {
        this.key = key;
        this.relativePath = relativePath;
        this.etag = etag;
        this.size = size;
    }

    @Override
    public String toString() {
        return "ObjectFile{" + "key='" + key + '\'' + ", relativePath='" + relativePath + '\'' + ", etag='" + etag
                + '\'' + ", size=" + size + '}';
    }
}
