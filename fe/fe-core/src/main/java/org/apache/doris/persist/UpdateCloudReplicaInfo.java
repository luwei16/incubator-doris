// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.persist;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UpdateCloudReplicaInfo implements Writable {
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "partitionId")
    private long partitionId;
    @SerializedName(value = "indexId")
    private long indexId;
    @SerializedName(value = "tabletId")
    private long tabletId;
    @SerializedName(value = "replicaId")
    private long replicaId;

    @SerializedName(value = "clusterId")
    private String clusterId;
    @SerializedName(value = "beId")
    private long beId;

    public UpdateCloudReplicaInfo() {
    }

    public UpdateCloudReplicaInfo(long dbId, long tableId, long partitionId, long indexId,
            long tabletId, long replicaId, String clusterId, long beId) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.indexId = indexId;
        this.tabletId = tabletId;
        this.replicaId = replicaId;
        this.clusterId = clusterId;
        this.beId = beId;
    }

    /*
    public void write(DataOutput out) throws IOException {
        out.writeLong(dbId);
        out.writeLong(tableId);
        out.writeLong(partitionId);
        out.writeLong(indexId);
        out.writeLong(tabletId);
        out.writeLong(replicaId);
        Text.writeString(out, clusterId);
        out.writeLong(beId);
    }

    public void readFields(DataInput in) throws IOException {
        dbId = in.readLong();
        tableId = in.readLong();
        partitionId = in.readLong();
        indexId = in.readLong();
        tabletId = in.readLong();
        replicaId = in.readLong();
        clusterId = Text.readString(in);
        beId = in.readLong();
    }

    public static UpdateCloudReplicaInfo read(DataInput in) throws IOException {
        UpdateCloudReplicaInfo replicaInfo = new UpdateCloudReplicaInfo();
        replicaInfo.readFields(in);
        return replicaInfo;
    }
    */

    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this, UpdateCloudReplicaInfo.class);
        Text.writeString(out, json);
    }

    public static UpdateCloudReplicaInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, UpdateCloudReplicaInfo.class);
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public long getIndexId() {
        return indexId;
    }

    public long getTabletId() {
        return tabletId;
    }

    public long getReplicaId() {
        return replicaId;
    }

    public String getClusterId() {
        return clusterId;
    }

    public long getBeId() {
        return beId;
    }
}
