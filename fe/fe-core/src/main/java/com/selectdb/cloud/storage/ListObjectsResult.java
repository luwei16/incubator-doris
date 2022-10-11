package com.selectdb.cloud.storage;

import lombok.Getter;

import java.util.List;

public class ListObjectsResult {
    @Getter
    private List<ObjectFile> objectInfoList;

    @Getter
    private boolean isTruncated;

    @Getter
    private String continuationToken;

    public ListObjectsResult(List<ObjectFile> objectInfoList, boolean isTruncated, String continuationToken) {
        this.objectInfoList = objectInfoList;
        this.isTruncated = isTruncated;
        this.continuationToken = continuationToken;
    }
}
