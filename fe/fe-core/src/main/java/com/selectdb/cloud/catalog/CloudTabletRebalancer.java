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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.persist.UpdateCloudReplicaInfo;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CloudTabletRebalancer extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CloudTabletRebalancer.class);

    Map<Long, List<Tablet>> beToTablets;

    public CloudTabletRebalancer() {
        super("cloud tablet rebalancer", Config.tablet_rebalancer_interval_second * 1000);
    }

    @Override
    protected void runAfterCatalogReady() {
        LOG.info("cloud tablet rebalance begin");
        beToTablets = new HashMap<Long, List<Tablet>>();
        Map<String, List<Long>> clusterToBes = new HashMap<String, List<Long>>();

        long start = System.currentTimeMillis();
        List<Long> dbIds = Env.getCurrentInternalCatalog().getDbIds();
        start = System.currentTimeMillis();
        for (Long dbId : dbIds) {
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }
            List<Table> tableList = db.getTables();
            for (Table table : tableList) {
                if (table.getType() != TableType.OLAP) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) table;
                if (!table.writeLockIfExist()) {
                    continue;
                }
                try {
                    for (Partition partition : olapTable.getAllPartitions()) {
                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            for (Tablet tablet : index.getTablets()) {
                                for (Replica replica : tablet.getReplicas()) {
                                    Map<String, List<Long>> clusterToBackends =
                                            ((CloudReplica) replica).getClusterToBackends();
                                    for (List<Long> bes : clusterToBackends.values()) {
                                        beToTablets.putIfAbsent(bes.get(0), new ArrayList<Tablet>());
                                        beToTablets.get(bes.get(0)).add(tablet);
                                    }
                                }
                            } // end for tablets
                        } // end for indices
                    } // end for partitions
                } finally {
                    table.writeUnlock();
                }
            }
        }

        SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
        for (Long beId : systemInfoService.getBackendIds(true)) {
            Backend be = systemInfoService.getBackend(beId);
            clusterToBes.putIfAbsent(be.getCloudClusterName(), new ArrayList<Long>());
            clusterToBes.get(be.getCloudClusterName()).add(beId);
        }

        LOG.info("cluster to backends {}", clusterToBes);

        for (Map.Entry<Long, List<Tablet>> entry : beToTablets.entrySet()) {
            LOG.info("before balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
        }

        for (Map.Entry<String, List<Long>> entry : clusterToBes.entrySet()) {
            balanceCluster(entry.getValue(), entry.getKey(), Config.balance_tablet_num_per_run);
        }

        for (Map.Entry<Long, List<Tablet>> entry : beToTablets.entrySet()) {
            LOG.info("after balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
        }

        LOG.info("finished to rebalancer. cost: {} ms", (System.currentTimeMillis() - start));
    }

    public void balanceCluster(List<Long> bes, String clusterId, int num) {
        if (bes == null || bes.isEmpty() || beToTablets == null || beToTablets.isEmpty()) {
            return;
        }

        for (int i = 0; i < num; i++) {
            long minBe = bes.get(0);
            long maxBe = bes.get(0);

            long minTabletsNum = Long.MAX_VALUE;
            long maxTabletsNum = 0;
            long totalTabletsNum = 0;
            long beNum = bes.size();

            for (Long be : bes) {
                long tabletNum = beToTablets.get(be) == null ? 0 : beToTablets.get(be).size();
                if (tabletNum > maxTabletsNum) {
                    maxBe = be;
                    maxTabletsNum = tabletNum;
                }

                if (tabletNum < minTabletsNum) {
                    minBe = be;
                    minTabletsNum = tabletNum;
                }
                totalTabletsNum += tabletNum;
            }

            long avgNum = totalTabletsNum / beNum;
            if ((maxTabletsNum < avgNum * (1 + Config.cloud_rebalance_percent_threshold)
                        || maxTabletsNum < avgNum + Config.cloud_rebalance_number_threshold)
                    && (minTabletsNum > avgNum * (1 - Config.cloud_rebalance_percent_threshold)
                        || minTabletsNum > avgNum - Config.cloud_rebalance_number_threshold)) {
                return;
            }

            Tablet pickedTablet = beToTablets.get(maxBe).get(0);

            // update clusterToBackens
            CloudReplica cloudReplica = (CloudReplica) pickedTablet.getReplicas().get(0);
            cloudReplica.updateClusterToBe(clusterId, minBe);
            LOG.info("cloud rebalancer transfer {} from to {} cluster {}", pickedTablet.getId(), maxBe, minBe,
                    clusterId, minTabletsNum, maxTabletsNum, beNum, totalTabletsNum);

            beToTablets.get(maxBe).remove(0);

            beToTablets.putIfAbsent(minBe, new ArrayList<Tablet>());
            beToTablets.get(minBe).add(pickedTablet);

            UpdateCloudReplicaInfo info = new UpdateCloudReplicaInfo(cloudReplica.getDbId(),
                    cloudReplica.getTableId(), cloudReplica.getPartitionId(), cloudReplica.getIndexId(),
                    pickedTablet.getId(), cloudReplica.getId(), clusterId, minBe);

            Env.getCurrentEnv().getEditLog().logUpdateCloudReplica(info);
        }
    }
}

