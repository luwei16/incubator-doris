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

package org.apache.doris.catalog;

import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CloudReplica extends Replica {
    private static final Logger LOG = LogManager.getLogger(CloudReplica.class);

    public CloudReplica(long replicaId, List<Long> backendIds, ReplicaState state, long version, int schemaHash) {
        super(replicaId, -1, state, version, schemaHash);
        clusterToBackends = new HashMap<String, List<Long>>();
    }

    @Override
    public long getBackendId() {
        String cluster = ConnectContext.get().getCloudCluster();
        if (cluster == null || cluster.isEmpty()) {
            cluster = Env.getCurrentSystemInfo().getCloudClusterNames().stream()
                                                    .filter(i -> !i.isEmpty()).findFirst().get();
        }

        if (clusterToBackends.containsKey(cluster)) {
            long backendId = clusterToBackends.get(cluster).get(0);
            Backend be = Env.getCurrentSystemInfo().getBackend(backendId);
            if (be.isQueryAvailable()) {
                LOG.info("lw test be id {} ", backendId);
                return backendId;
            }
        }

        // TODO(luwei) list shoule be sorted
        List<Backend> availableBes = Env.getCurrentSystemInfo().getBackendsByClusterName(cluster);
        LOG.info("lw test backends {} ", availableBes);
        long index = getId() % availableBes.size();

        // save to clusterToBackends map
        List<Long> bes = new ArrayList<Long>();
        bes.add(availableBes.get((int) index).getId());
        LOG.info("lw test backends {} ", availableBes.get((int) index).getId());
        clusterToBackends.put(cluster, bes);

        LOG.info("lw test be id {} ", availableBes.get((int) index).getId());
        return availableBes.get((int) index).getId();
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

    // In the future, a replica may be mapped to multiple BEs in a cluster,
    // so this value is be list
    private Map<String, List<Long>> clusterToBackends;
}
