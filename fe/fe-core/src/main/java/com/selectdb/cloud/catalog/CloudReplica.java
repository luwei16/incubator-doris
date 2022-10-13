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

package com.selectdb.cloud.catalog;

import com.google.gson.annotations.SerializedName;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CloudReplica extends Replica {
    private static final Logger LOG = LogManager.getLogger(CloudReplica.class);

    // In the future, a replica may be mapped to multiple BEs in a cluster,
    // so this value is be list
    private Map<String, List<Long>> clusterToBackends = new HashMap<String, List<Long>>();
    @SerializedName(value = "dbId")
    private long dbId = -1;
    @SerializedName(value = "tableId")
    private long tableId = -1;
    @SerializedName(value = "partitionId")
    private long partitionId = -1;
    @SerializedName(value = "indexId")
    private long indexId = -1;
    @SerializedName(value = "idx")
    private long idx = -1;

    public CloudReplica() {
    }

    public CloudReplica(long replicaId, List<Long> backendIds, ReplicaState state, long version, int schemaHash,
            long dbId, long tableId, long partitionId, long indexId, long idx) {
        super(replicaId, -1, state, version, schemaHash);
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.indexId = indexId;
        this.idx = idx;
    }

    private boolean isColocated() {
        return idx != -1;
    }

    private long getColocatedBeId(String cluster) {
        List<Backend> availableBes = Env.getCurrentSystemInfo().getBackendsByClusterName(cluster);

        // Tablets with the same idx will be hashed to the same BE, which
        // meets the requirements of colocated table.
        long index = idx % availableBes.size();
        long pickedBeId = availableBes.get((int) index).getId();

        return pickedBeId;
    }

    @Override
    public long getBackendId() {
        String cluster = null;
        // Not in a connect session
        if (ConnectContext.get() != null) {
            cluster = ConnectContext.get().getCloudCluster();
        }

        if (cluster == null || cluster.isEmpty()) {
            try {
                cluster = Env.getCurrentSystemInfo().getCloudClusterNames().stream()
                                                    .filter(i -> !i.isEmpty()).findFirst().get();
                if (ConnectContext.get() != null) {
                    ConnectContext.get().setCloudCluster(cluster);
                }
            } catch (Exception e) {
                LOG.warn("failed to get cluster, clusterNames={}",
                         Env.getCurrentSystemInfo().getCloudClusterNames(), e);
                return -1;
            }
        }

        // No cluster right now
        if (cluster == null || cluster.isEmpty()) {
            LOG.warn("lw test get backend, return -1");
            return -1;
        }

        if (isColocated()) {
            return getColocatedBeId(cluster);
        }

        if (clusterToBackends.containsKey(cluster)) {
            long backendId = clusterToBackends.get(cluster).get(0);
            Backend be = Env.getCurrentSystemInfo().getBackend(backendId);
            if (be != null && be.isQueryAvailable()) {
                LOG.debug("backendId={} ", backendId);
                return backendId;
            }
        }

        // TODO(luwei) list shoule be sorted
        List<Backend> clusterBes = Env.getCurrentSystemInfo().getBackendsByClusterName(cluster);
        // use alive be to exec sql
        List<Backend> availableBes = new ArrayList<>();
        for (Backend be : clusterBes) {
            if (be.isAlive()) {
                availableBes.add(be);
            }
        }
        if (availableBes == null || availableBes.size() == 0) {
            return -1;
        }
        LOG.debug("availableBes={}", availableBes);
        long index = getId() % availableBes.size();
        long pickedBeId = availableBes.get((int) index).getId();
        LOG.debug("picked backendId={}", pickedBeId);

        // save to clusterToBackends map
        List<Long> bes = new ArrayList<Long>();
        bes.add(pickedBeId);
        clusterToBackends.put(cluster, bes);

        return pickedBeId;
    }

    @Override
    public boolean checkVersionCatchUp(long expectedVersion, boolean ignoreAlter) {
        if (ignoreAlter && getState() == ReplicaState.ALTER
                && getVersion() == Partition.PARTITION_INIT_VERSION) {
            return true;
        }

        if (expectedVersion == Partition.PARTITION_INIT_VERSION) {
            // no data is loaded into this replica, just return true
            return true;
        }

        return true;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        dbId = in.readLong();
        tableId = in.readLong();
        partitionId = in.readLong();
        indexId = in.readLong();
        idx = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeLong(dbId);
        out.writeLong(tableId);
        out.writeLong(partitionId);
        out.writeLong(indexId);
        out.writeLong(idx);
    }

    public static CloudReplica read(DataInput in) throws IOException {
        CloudReplica replica = new CloudReplica();
        replica.readFields(in);
        // TODO(luwei): perisist and fillup clusterToBackends to take full advantage of data cache
        return replica;
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

    public long getIdx() {
        return idx;
    }
}
