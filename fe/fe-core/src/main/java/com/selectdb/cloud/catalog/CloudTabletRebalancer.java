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

import com.selectdb.cloud.proto.SelectdbCloud;
import com.selectdb.cloud.rpc.MetaServiceProxy;

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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.persist.UpdateCloudReplicaInfo;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPreCacheAsyncRequest;
import org.apache.doris.thrift.TPreCacheAsyncResponse;
import org.apache.doris.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

public class CloudTabletRebalancer extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CloudTabletRebalancer.class);

    private Map<Long, List<Tablet>> beToTabletsGlobal;
    private Map<String, List<Long>> clusterToBes;
    private List<UpdateCloudReplicaInfo> replicaInfos;

    // partitionId -> indexId -> be -> tablet
    private Map<Long, Map<Long, Map<Long, List<Tablet>>>> partitionToTablets;

    private Map<Long, Long> beToDecommissionedTime = new HashMap<Long, Long>();

    private Random rand = new Random();

    private boolean indexBalanced = true;

    private LinkedBlockingQueue<Pair<Long, Long>> tabletsMigrateTasks = new LinkedBlockingQueue<Pair<Long, Long>>();

    public CloudTabletRebalancer() {
        super("cloud tablet rebalancer", Config.tablet_rebalancer_interval_second * 1000);
    }

    private interface Operator {
        void op(Database db, Table table, Partition partition, MaterializedIndex index, String cluster);
    }

    // 1 build cluster to backends info
    // 2 complete route info
    // 3 Statistics cluster to backend mapping information
    // 4 balance in partitions/index
    // 5 if the partition already balanced, perform global balance
    @Override
    protected void runAfterCatalogReady() {
        LOG.info("cloud tablet rebalance begin");
        beToTabletsGlobal = new HashMap<Long, List<Tablet>>();
        partitionToTablets = new HashMap<Long, Map<Long, Map<Long, List<Tablet>>>>();
        clusterToBes = new HashMap<String, List<Long>>();
        long start = System.currentTimeMillis();

        // build cluster to backend info
        SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
        for (Long beId : systemInfoService.getBackendIds(false)) {
            LOG.info("lw test beid {}", beId);
            Backend be = systemInfoService.getBackend(beId);
            clusterToBes.putIfAbsent(be.getCloudClusterName(), new ArrayList<Long>());
            clusterToBes.get(be.getCloudClusterName()).add(beId);
        }
        LOG.info("cluster to backends {}", clusterToBes);

        // complete route info
        replicaInfos = new ArrayList<UpdateCloudReplicaInfo>();
        completeRouteInfo();
        for (UpdateCloudReplicaInfo info : replicaInfos) {
            Env.getCurrentEnv().getEditLog().logUpdateCloudReplica(info);
        }

        // Statistics cluster to be mapping information
        statRouteInfo();

        Pair<Long, Long> pair;
        while (!tabletsMigrateTasks.isEmpty()) {
            try {
                pair = tabletsMigrateTasks.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            LOG.info("begin tablets migration from be {} to be {}", pair.first, pair.second);
            migrateTablets(pair.first, pair.second);
        }

        for (Map.Entry<Long, List<Tablet>> entry : beToTabletsGlobal.entrySet()) {
            LOG.info("before index balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
        }

        // balance in partitions/index
        indexBalanced = true;
        for (Map.Entry<String, List<Long>> entry : clusterToBes.entrySet()) {
            balanceInPartition(entry.getValue(), entry.getKey());
        }

        for (Map.Entry<Long, List<Tablet>> entry : beToTabletsGlobal.entrySet()) {
            LOG.info("after index balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
        }

        // if the partition/index already balanced, perform global balance
        if (indexBalanced) {
            for (Map.Entry<Long, List<Tablet>> entry : beToTabletsGlobal.entrySet()) {
                LOG.info("before global balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
            }

            for (Map.Entry<String, List<Long>> entry : clusterToBes.entrySet()) {
                balanceImpl(entry.getValue(), entry.getKey(), beToTabletsGlobal, true);
            }

            for (Map.Entry<Long, List<Tablet>> entry : beToTabletsGlobal.entrySet()) {
                LOG.info("after global balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
            }
        }

        checkDecommissionState(clusterToBes);

        LOG.info("finished to rebalancer. cost: {} ms", (System.currentTimeMillis() - start));
    }

    public void checkDecommissionState(Map<String, List<Long>> clusterToBes) {
        for (Map.Entry<String, List<Long>> entry : clusterToBes.entrySet()) {
            List<Long> beList = entry.getValue();
            for (long beId : beList) {
                long tabletNum = beToTabletsGlobal.get(beId) == null ? 0 : beToTabletsGlobal.get(beId).size();
                Backend backend = Env.getCurrentSystemInfo().getBackend(beId);
                if (backend.isDecommissioned() && tabletNum == 0) {
                    if (!beToDecommissionedTime.containsKey(beId)) {
                        SelectdbCloud.AlterClusterRequest.Builder builder =
                                SelectdbCloud.AlterClusterRequest.newBuilder();
                        builder.setCloudUniqueId(Config.cloud_unique_id);
                        builder.setOp(SelectdbCloud.AlterClusterRequest.Operation.NOTIFY_DECOMMISSIONED);

                        SelectdbCloud.ClusterPB.Builder clusterBuilder =
                                SelectdbCloud.ClusterPB.newBuilder();
                        clusterBuilder.setClusterName(backend.getCloudClusterName());
                        clusterBuilder.setClusterId(backend.getCloudClusterId());
                        clusterBuilder.setType(SelectdbCloud.ClusterPB.Type.COMPUTE);

                        SelectdbCloud.NodeInfoPB.Builder nodeBuilder =
                                SelectdbCloud.NodeInfoPB.newBuilder();
                        nodeBuilder.setIp(backend.getHost());
                        nodeBuilder.setHeartbeatPort(backend.getHeartbeatPort());
                        nodeBuilder.setCloudUniqueId(backend.getCloudUniqueId());
                        nodeBuilder.setStatus(SelectdbCloud.NodeStatusPB.NODE_STATUS_DECOMMISSIONED);

                        clusterBuilder.addNodes(nodeBuilder);
                        builder.setCluster(clusterBuilder);

                        SelectdbCloud.AlterClusterResponse response;
                        try {
                            response = MetaServiceProxy.getInstance().alterCluster(builder.build());
                            if (response.getStatus().getCode() != SelectdbCloud.MetaServiceCode.OK) {
                                LOG.warn("notify decommission response: {}", response);
                            }
                            LOG.info("notify decommission response: {} ", response);
                        } catch (RpcException e) {
                            LOG.info("failed to notify decommission {}", e);
                            return;
                        }
                        beToDecommissionedTime.put(beId, System.currentTimeMillis() / 1000);
                    }
                }
            }
        }
    }

    private void completeRouteInfo() {
        loopCloudReplica((Database db, Table table, Partition partition, MaterializedIndex index, String cluster) -> {
            boolean assigned = false;
            List<Long> beIds = new ArrayList<Long>();
            List<Long> tabletIds = new ArrayList<Long>();
            for (Tablet tablet : index.getTablets()) {
                for (Replica replica : tablet.getReplicas()) {
                    Map<String, List<Long>> clusterToBackends =
                            ((CloudReplica) replica).getClusterToBackends();
                    if (!clusterToBackends.containsKey(cluster)) {
                        long beId = ((CloudReplica) replica).hashReplicaToBe(cluster);
                        List<Long> bes = new ArrayList<Long>();
                        bes.add(beId);
                        clusterToBackends.put(cluster, bes);

                        assigned = true;
                        beIds.add(beId);
                        tabletIds.add(tablet.getId());
                    } else {
                        beIds.add(clusterToBackends.get(cluster).get(0));
                        tabletIds.add(tablet.getId());
                    }
                }
            }

            if (assigned) {
                UpdateCloudReplicaInfo info = new UpdateCloudReplicaInfo(db.getId(), table.getId(),
                        partition.getId(), index.getId(), cluster, beIds, tabletIds);
                replicaInfos.add(info);
            }
        });
    }

    public void statRouteInfo() {
        loopCloudReplica((Database db, Table table, Partition partition, MaterializedIndex index, String cluster) -> {
            for (Tablet tablet : index.getTablets()) {
                for (Replica replica : tablet.getReplicas()) {
                    Map<String, List<Long>> clusterToBackends =
                            ((CloudReplica) replica).getClusterToBackends();
                    for (List<Long> bes : clusterToBackends.values()) {
                        beToTabletsGlobal.putIfAbsent(bes.get(0), new ArrayList<Tablet>());
                        beToTabletsGlobal.get(bes.get(0)).add(tablet);

                        partitionToTablets.putIfAbsent(partition.getId(),
                                                       new HashMap<Long, Map<Long, List<Tablet>>>());
                        Map<Long, Map<Long, List<Tablet>>> indexToTablets
                                = partitionToTablets.get(partition.getId());
                        indexToTablets.putIfAbsent(index.getId(),
                                                   new HashMap<Long, List<Tablet>>());
                        Map<Long, List<Tablet>> beToTabletsOfIndex
                                = indexToTablets.get(index.getId());
                        beToTabletsOfIndex.putIfAbsent(bes.get(0), new ArrayList<Tablet>());
                        beToTabletsOfIndex.get(bes.get(0)).add(tablet);
                    }
                }
            }
        });
    }

    public void loopCloudReplica(Operator operator) {
        List<Long> dbIds = Env.getCurrentInternalCatalog().getDbIds();
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
                            for (Map.Entry<String, List<Long>> entry : clusterToBes.entrySet()) {
                                String cluster = entry.getKey();
                                operator.op(db, table, partition, index, cluster);
                            }
                        } // end for indices
                    } // end for partitions
                } finally {
                    table.writeUnlock();
                }
            }
        }
    }

    public void balanceInPartition(List<Long> bes, String clusterId) {
        // balance all partition
        for (Map.Entry<Long, Map<Long, Map<Long, List<Tablet>>>> partitionEntry : partitionToTablets.entrySet()) {
            Map<Long, Map<Long, List<Tablet>>> indexToTablets = partitionEntry.getValue();
            // balance all index of a partition
            for (Map.Entry<Long, Map<Long, List<Tablet>>> entry : indexToTablets.entrySet()) {
                LOG.info("balance partttion {} Index {}, cluster {}", partitionEntry.getKey(),
                        entry.getKey(), clusterId);
                // balance a index
                balanceImpl(bes, clusterId, entry.getValue(), false);
            }
        }
    }

    private void balanceImpl(List<Long> bes, String clusterId, Map<Long, List<Tablet>> beToTablets,
            boolean isGlobal) {
        if (bes == null || bes.isEmpty() || beToTablets == null || beToTablets.isEmpty()) {
            return;
        }

        long totalTabletsNum = 0;
        long beNum = 0;
        for (Long be : bes) {
            long tabletNum = beToTablets.get(be) == null ? 0 : beToTablets.get(be).size();
            Backend backend = Env.getCurrentSystemInfo().getBackend(be);
            if (!backend.isDecommissioned()) {
                beNum++;
            }
            totalTabletsNum += tabletNum;
        }
        long avgNum = totalTabletsNum / beNum;
        long transferNum = Math.max(Math.round(avgNum * Config.balance_tablet_percent_per_run),
                                    Config.min_balance_tablet_num_per_run);

        for (int i = 0; i < transferNum; i++) {
            long destBe = bes.get(0);
            long srcBe = bes.get(0);

            long minTabletsNum = Long.MAX_VALUE;
            long maxTabletsNum = 0;
            boolean srcDecommissioned = false;

            for (Long be : bes) {
                long tabletNum = beToTablets.get(be) == null ? 0 : beToTablets.get(be).size();
                if (tabletNum > maxTabletsNum) {
                    srcBe = be;
                    maxTabletsNum = tabletNum;
                }

                Backend backend = Env.getCurrentSystemInfo().getBackend(be);
                if (tabletNum < minTabletsNum && !backend.isDecommissioned()) {
                    destBe = be;
                    minTabletsNum = tabletNum;
                }
            }

            for (Long be : bes) {
                long tabletNum = beToTablets.get(be) == null ? 0 : beToTablets.get(be).size();
                Backend backend = Env.getCurrentSystemInfo().getBackend(be);
                if (backend.isDecommissioned() && tabletNum > 0) {
                    srcBe = be;
                    srcDecommissioned = true;
                    break;
                }
            }

            if (!srcDecommissioned) {
                if ((maxTabletsNum < avgNum * (1 + Config.cloud_rebalance_percent_threshold)
                        && minTabletsNum > avgNum * (1 - Config.cloud_rebalance_percent_threshold))
                        || minTabletsNum > maxTabletsNum - Config.cloud_rebalance_number_threshold) {
                    return;
                }
            }

            int randomIndex = rand.nextInt(beToTablets.get(srcBe).size());
            Tablet pickedTablet = beToTablets.get(srcBe).get(randomIndex);
            CloudReplica cloudReplica = (CloudReplica) pickedTablet.getReplicas().get(0);

            if (isGlobal) {
                // update clusterToBackens
                long maxBeSize = partitionToTablets.get(cloudReplica.getPartitionId())
                        .get(cloudReplica.getIndexId()).get(srcBe).size();
                long minBeSize = partitionToTablets.get(cloudReplica.getPartitionId())
                        .get(cloudReplica.getIndexId()).get(destBe).size();
                if (minBeSize >= maxBeSize) {
                    return;
                }
                cloudReplica.updateClusterToBe(clusterId, destBe);
                LOG.info("transfer {} from {} to {}, cluster {} minNum {} maxNum {} beNum {} totalTabletsNum {}",
                         pickedTablet.getId(), srcBe, destBe, clusterId,
                         minTabletsNum, maxTabletsNum, beNum, totalTabletsNum);

                beToTabletsGlobal.get(srcBe).remove(randomIndex);
                beToTabletsGlobal.putIfAbsent(destBe, new ArrayList<Tablet>());
                beToTabletsGlobal.get(destBe).add(pickedTablet);
            } else {
                indexBalanced = false;
                // update clusterToBackens
                cloudReplica.updateClusterToBe(clusterId, destBe);
                LOG.info("transfer {} from {} to {} cluster {} minNum {} maxNum {} beNum {} totalTabletsNum {} ",
                         pickedTablet.getId(), srcBe, destBe, clusterId,
                         minTabletsNum, maxTabletsNum, beNum, totalTabletsNum);

                beToTabletsGlobal.get(srcBe).remove(randomIndex);
                beToTabletsGlobal.putIfAbsent(destBe, new ArrayList<Tablet>());
                beToTabletsGlobal.get(destBe).add(pickedTablet);

                beToTablets.get(srcBe).remove(randomIndex);
                beToTablets.putIfAbsent(destBe, new ArrayList<Tablet>());
                beToTablets.get(destBe).add(pickedTablet);
            }

            if (Config.preheating_enabled) {
                BackendService.Client client = null;
                TNetworkAddress address = null;
                Backend srcBackend = Env.getCurrentSystemInfo().getBackend(srcBe);
                Backend destBackend = Env.getCurrentSystemInfo().getBackend(destBe);
                try {
                    address = new TNetworkAddress(destBackend.getHost(), destBackend.getBePort());
                    client = ClientPool.backendPool.borrowObject(address);
                    TPreCacheAsyncRequest req = new TPreCacheAsyncRequest();
                    req.setHost(srcBackend.getHost());
                    req.setBrpcPort(srcBackend.getBrpcPort());
                    List<Long> tablets = new ArrayList<Long>();
                    tablets.add(pickedTablet.getId());
                    req.setTabletIds(tablets);
                    TPreCacheAsyncResponse result = client.preCacheAsync(req);
                    if (result.getStatus().getStatusCode() != TStatusCode.OK) {
                        LOG.warn("pre cache failed status {} {}", result.getStatus().getStatusCode(),
                                result.getStatus().getErrorMsgs());
                    } else {
                        LOG.info("pre cache succ status {} {}", result.getStatus().getStatusCode(),
                                result.getStatus().getErrorMsgs());
                    }
                } catch (Exception e) {
                    LOG.warn("task exec error. backend[{}]", destBackend.getId(), e);
                }
            }

            Database db = Env.getCurrentInternalCatalog().getDbNullable(cloudReplica.getDbId());
            if (db == null) {
                continue;
            }
            OlapTable table = (OlapTable) db.getTableNullable(cloudReplica.getTableId());
            if (table == null) {
                continue;
            }

            table.readLock();

            try {
                if (db.getTableNullable(cloudReplica.getTableId()) == null) {
                    continue;
                }

                UpdateCloudReplicaInfo info = new UpdateCloudReplicaInfo(cloudReplica.getDbId(),
                        cloudReplica.getTableId(), cloudReplica.getPartitionId(), cloudReplica.getIndexId(),
                        pickedTablet.getId(), cloudReplica.getId(), clusterId, destBe);
                Env.getCurrentEnv().getEditLog().logUpdateCloudReplica(info);
            } finally {
                table.readUnlock();
            }
        }
    }

    public void addTabletMigrationTask(Long srcBe, Long dstBe) {
        tabletsMigrateTasks.offer(Pair.of(srcBe, dstBe));
    }

    /* Migrate tablet replicas from srcBe to dstBe
     * replica location info will be updated in both master and follower FEs.
     */
    private void migrateTablets(Long srcBe, Long dstBe) {
        // get tablets
        List<Tablet> tablets = beToTabletsGlobal.get(srcBe);
        SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
        if (tablets.isEmpty()) {
            // srcBe does not have any tablets, set inactive
            Env.getCurrentEnv().getCloudUpgradeMgr().setBeStateInactive(srcBe);
            return;
        }
        for (Tablet tablet : tablets) {
            // get replica
            CloudReplica cloudReplica = (CloudReplica) tablet.getReplicas().get(0);
            String clusterId = systemInfoService.getBackend(srcBe).getCloudClusterId();
            String clusterName = systemInfoService.getBackend(srcBe).getCloudClusterName();
            // update replica location info
            cloudReplica.updateClusterToBe(clusterId, dstBe);
            LOG.info("cloud be migrate tablet {} from srcBe={} to dstBe={}, clusterId={}, clusterName={}",
                    tablet.getId(), srcBe, dstBe, clusterId, clusterName);

            // populate to followers
            Database db = Env.getCurrentInternalCatalog().getDbNullable(cloudReplica.getDbId());
            if (db == null) {
                LOG.error("get null db from replica, tabletId={}, partitionId={}, beId={}",
                        cloudReplica.getTableId(), cloudReplica.getPartitionId(), cloudReplica.getBackendId());
                continue;
            }
            OlapTable table = (OlapTable) db.getTableNullable(cloudReplica.getTableId());
            if (table == null) {
                continue;
            }

            table.readLock();
            try {
                if (db.getTableNullable(cloudReplica.getTableId()) == null) {
                    continue;
                }
                UpdateCloudReplicaInfo info = new UpdateCloudReplicaInfo(cloudReplica.getDbId(),
                        cloudReplica.getTableId(), cloudReplica.getPartitionId(), cloudReplica.getIndexId(),
                        tablet.getId(), cloudReplica.getId(), clusterId, dstBe);
                Env.getCurrentEnv().getEditLog().logUpdateCloudReplica(info);
            } finally {
                table.readUnlock();
            }
        }
        try {
            Env.getCurrentEnv().getCloudUpgradeMgr().registerWaterShedTxnId(srcBe);
        } catch (AnalysisException e) {
            throw new RuntimeException(e);
        }
    }
}

