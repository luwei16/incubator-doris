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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
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
    private long idx = -1;

    public CloudReplica() {
    }

    public CloudReplica(long replicaId, List<Long> backendIds, ReplicaState state, long version, int schemaHash) {
        super(replicaId, -1, state, version, schemaHash);
    }

    private boolean isColocated() {
        TabletInvertedIndex index = Env.getCurrentInvertedIndex();
        if (index == null || index.getTabletIdByReplica(getId()) == null) {
            return false;
        }
        long tableId = index.getTabletMeta(index.getTabletIdByReplica(getId())).getTableId();
        return Env.getCurrentColocateIndex().isColocateTable(tableId);
    }

    private long getIdx() {
        long tabletId = Env.getCurrentInvertedIndex().getTabletIdByReplica(getId());
        TabletMeta meta = Env.getCurrentInvertedIndex().getTabletMeta(tabletId);
        long tableId = meta.getTableId();
        long dbId =  meta.getDbId();

        Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
        OlapTable olapTable = (OlapTable) db.getTableNullable(tableId);

        olapTable.readLock();
        try {
            for (Partition partition : olapTable.getPartitions()) {
                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                    int idx = 0;
                    for (Long id : index.getTabletIdsInOrder()) {
                        if (tabletId == id) {
                            return idx;
                        }
                        idx++;
                    }
                }
            }
        } finally {
            olapTable.readUnlock();
        }

        return -1;
    }

    private long getColocatedBeId(String cluster) {
        if (idx == -1) {
            idx = getIdx();
        }

        List<Backend> availableBes = Env.getCurrentSystemInfo().getBackendsByClusterName(cluster);

        // Tablets with the same idx will be hashed to the same BE, which
        // meets the requirements of colocated table.
        long index = idx % availableBes.size();
        long pickedBeId = availableBes.get((int) index).getId();

        LOG.info("lw test idx {} {} {}", idx, getId(), pickedBeId);
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
            if (be.isQueryAvailable()) {
                LOG.debug("backendId={} ", backendId);
                return backendId;
            }
        }

        // TODO(luwei) list shoule be sorted
        List<Backend> availableBes = Env.getCurrentSystemInfo().getBackendsByClusterName(cluster);
        LOG.debug("availableBes={} ", availableBes);
        long index = getId() % availableBes.size();
        long pickedBeId = availableBes.get((int) index).getId();
        LOG.info("picked backendId={} ", pickedBeId);

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

    public static CloudReplica read(DataInput in) throws IOException {
        CloudReplica replica = new CloudReplica();
        replica.readFields(in);
        // TODO(luwei): perisist and fillup clusterToBackends to take full advantage of data cache
        return replica;
    }
}
