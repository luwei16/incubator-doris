package com.selectdb.cloud.catalog;

import com.selectdb.cloud.proto.SelectdbCloud;
import com.selectdb.cloud.proto.SelectdbCloud.ClusterPB;
import com.selectdb.cloud.proto.SelectdbCloud.ClusterPB.Type;
import com.selectdb.cloud.proto.SelectdbCloud.MetaServiceCode;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.metric.GaugeMetricImpl;
import org.apache.doris.metric.Metric.MetricUnit;
import org.apache.doris.metric.MetricLabel;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Frontend;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class CloudClusterChecker extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CloudClusterChecker.class);

    public CloudClusterChecker() {
        super("cloud cluster check", FeConstants.cloud_cluster_check_interval_second * 1000L);
    }

    /**
     * Diff 2 collections of current and the dest.
     * @param toAdd output param = (expectedState - currentState)
     * @param toDel output param = (currentState - expectedState)
     * @param supplierCurrentMapFunc get the current be or fe objects information map from memory, a lambda function
     * @param supplierNodeMapFunc get be or fe information map from meta_service return pb, a lambda function
     */
    private <T> void diffNodes(List<T> toAdd, List<T> toDel, Supplier<Map<String, T>> supplierCurrentMapFunc,
                               Supplier<Map<String, T>> supplierNodeMapFunc) {
        if (toAdd == null || toDel == null) {
            return;
        }

        // TODO(gavin): Consider VPC
        // vpc:ip:port -> Nodes
        Map<String, T> currentMap = supplierCurrentMapFunc.get();
        Map<String, T> nodeMap = supplierNodeMapFunc.get();

        LOG.debug("current Nodes={} expected Nodes={}", currentMap.keySet(), nodeMap.keySet());

        toDel.addAll(currentMap.keySet().stream().filter(i -> !nodeMap.containsKey(i))
                .map(i -> currentMap.get(i)).collect(Collectors.toList()));

        toAdd.addAll(nodeMap.keySet().stream().filter(i -> !currentMap.containsKey(i))
                .map(i -> nodeMap.get(i)).collect(Collectors.toList()));
    }

    private void checkToAddCluster(Map<String, ClusterPB> remoteClusterIdToPB, Set<String> localClusterIds) {
        List<String> toAddClusterIds = remoteClusterIdToPB.keySet().stream()
                .filter(i -> !localClusterIds.contains(i)).collect(Collectors.toList());
        toAddClusterIds.forEach(
                addId -> {
                    LOG.debug("begin to add clusterId: {}", addId);
                    /*
                    List<Backend> toAdd = new ArrayList<>();
                    for (SelectdbCloud.NodeInfoPB node : remoteClusterIdToPB.get(addId).getNodesList()) {
                        Backend b = new Backend(Env.getCurrentEnv().getNextId(), node.getIp(), node.getHeartbeatPort());
                        toAdd.add(b);
                    }
                    */
                    // Attach tag to BEs
                    Map<String, String> newTagMap = Tag.DEFAULT_BACKEND_TAG.toMap();
                    String clusterName = remoteClusterIdToPB.get(addId).getClusterName();
                    String clusterId = remoteClusterIdToPB.get(addId).getClusterId();
                    newTagMap.put(Tag.CLOUD_CLUSTER_NAME, clusterName);
                    newTagMap.put(Tag.CLOUD_CLUSTER_ID, clusterId);
                    MetricRepo.registerClusterMetrics(clusterName, clusterId);
                    //toAdd.forEach(i -> i.setTagMap(newTagMap));
                    List<Backend> toAdd = new ArrayList<>();
                    for (SelectdbCloud.NodeInfoPB node : remoteClusterIdToPB.get(addId).getNodesList()) {
                        Backend b = new Backend(Env.getCurrentEnv().getNextId(), node.getIp(), node.getHeartbeatPort());
                        newTagMap.put(Tag.CLOUD_UNIQUE_ID, node.getCloudUniqueId());
                        b.setTagMap(newTagMap);
                        toAdd.add(b);
                    }
                    Env.getCurrentSystemInfo().updateCloudBackends(toAdd, new ArrayList<>());
                }
        );
    }

    private void checkToDelCluster(Map<String, ClusterPB> remoteClusterIdToPB, Set<String> localClusterIds,
                                   Map<String, List<Backend>> clusterIdToBackend) {
        List<String> toDelClusterIds = localClusterIds.stream()
                .filter(i -> !remoteClusterIdToPB.containsKey(i)).collect(Collectors.toList());
        // drop be cluster
        Map<String, List<Backend>> finalClusterIdToBackend = clusterIdToBackend;
        toDelClusterIds.forEach(
                delId -> {
                    LOG.debug("begin to drop clusterId: {}", delId);
                    List<Backend> toDel =
                            new ArrayList<>(finalClusterIdToBackend.getOrDefault(delId, new ArrayList<>()));
                    Env.getCurrentSystemInfo().updateCloudBackends(new ArrayList<>(), toDel);
                    // del clusterName
                    String delClusterName = Env.getCurrentSystemInfo().getClusterNameByClusterId(delId);
                    if (delClusterName.isEmpty()) {
                        LOG.warn("can't get delClusterName, clusterId: {}, plz check", delId);
                        return;
                    }
                    // del clusterID
                    Env.getCurrentSystemInfo().dropCluster(delId, delClusterName);
                }
        );
    }

    private void updateStatus(List<Backend> currentBes, List<SelectdbCloud.NodeInfoPB> expectedBes) {
        Map<String, Backend> currentMap = new HashMap<>();
        for (Backend be : currentBes) {
            String endpoint = be.getHost() + ":" + be.getHeartbeatPort();
            currentMap.put(endpoint, be);
        }

        for (SelectdbCloud.NodeInfoPB node : expectedBes) {
            String endpoint = node.getIp() + ":" + node.getHeartbeatPort();
            SelectdbCloud.NodeStatusPB status = node.getStatus();
            Backend be = currentMap.get(endpoint);

            if (status == SelectdbCloud.NodeStatusPB.NODE_STATUS_DECOMMISSIONING) {
                if (!be.isDecommissioned()) {
                    LOG.info("decommissioned backend: {} status: {}", be, status);
                    try {
                        Env.getCurrentEnv().getCloudUpgradeMgr().registerWaterShedTxnId(be.getId());
                    } catch (AnalysisException e) {
                        LOG.warn("failed to register water shed txn id, decommission be {}", be.getId(), e);
                    }
                    be.setDecommissioned(true);
                }
            }
        }
    }

    private void checkDiffNode(Map<String, ClusterPB> remoteClusterIdToPB,
                               Map<String, List<Backend>> clusterIdToBackend) {
        for (String cid : clusterIdToBackend.keySet()) {
            List<Backend> toAdd = new ArrayList<>();
            List<Backend> toDel = new ArrayList<>();
            ClusterPB cp = remoteClusterIdToPB.get(cid);
            if (cp == null) {
                LOG.warn("can't get cid {} info, and local cluster info {}, remote cluster info {}",
                        cid, clusterIdToBackend, remoteClusterIdToPB);
                continue;
            }
            String newClusterName = cp.getClusterName();
            List<Backend> currentBes = clusterIdToBackend.getOrDefault(cid, new ArrayList<>());
            String currentClusterName = currentBes.stream().map(Backend::getCloudClusterName).findFirst().orElse("");

            if (!newClusterName.equals(currentClusterName)) {
                // rename cluster's name
                LOG.info("cluster_name corresponding to cluster_id has been changed,"
                        + " cluster_id : {} , current_cluster_name : {}, new_cluster_name :{}",
                        cid, currentClusterName, newClusterName);
                // change all be's cluster_name
                currentBes.forEach(b -> b.setCloudClusterName(newClusterName));
                // update clusterNameToId
                Env.getCurrentSystemInfo().updateClusterNameToId(newClusterName, currentClusterName, cid);
                // update tags
                currentBes.forEach(b -> Env.getCurrentEnv().getEditLog().logModifyBackend(b));
            }

            List<String> currentBeEndpoints = currentBes.stream().map(backend ->
                    backend.getHost() + ":" + backend.getHeartbeatPort()).collect(Collectors.toList());
            List<SelectdbCloud.NodeInfoPB> expectedBes = remoteClusterIdToPB.get(cid).getNodesList();
            List<String> remoteBeEndpoints = expectedBes.stream()
                    .map(pb -> pb.getIp() + ":" + pb.getHeartbeatPort()).collect(Collectors.toList());
            LOG.info("get cloud cluster, clusterId={} local nodes={} remote nodes={}", cid,
                    currentBeEndpoints, remoteBeEndpoints);

            updateStatus(currentBes, expectedBes);

            // Attach tag to BEs
            Map<String, String> newTagMap = Tag.DEFAULT_BACKEND_TAG.toMap();
            newTagMap.put(Tag.CLOUD_CLUSTER_NAME, remoteClusterIdToPB.get(cid).getClusterName());
            newTagMap.put(Tag.CLOUD_CLUSTER_ID, remoteClusterIdToPB.get(cid).getClusterId());

            diffNodes(toAdd, toDel, () -> {
                Map<String, Backend> currentMap = new HashMap<>();
                for (Backend be : currentBes) {
                    String endpoint = be.getHost() + ":" + be.getHeartbeatPort();
                    currentMap.put(endpoint, be);
                }
                return currentMap;
            }, () -> {
                Map<String, Backend> nodeMap = new HashMap<>();
                for (SelectdbCloud.NodeInfoPB node : expectedBes) {
                    String endpoint = node.getIp() + ":" + node.getHeartbeatPort();
                    Backend b = new Backend(Env.getCurrentEnv().getNextId(), node.getIp(), node.getHeartbeatPort());
                    if (node.hasIsSmoothUpgrade()) {
                        b.setSmoothUpgradeDst(node.getIsSmoothUpgrade());
                    }
                    newTagMap.put(Tag.CLOUD_UNIQUE_ID, node.getCloudUniqueId());
                    b.setTagMap(newTagMap);
                    nodeMap.put(endpoint, b);
                }
                return nodeMap;
            });

            LOG.debug("cluster_id: {}, diffBackends nodes: {}, current: {}, toAdd: {}, toDel: {}",
                    cid, expectedBes, currentBes, toAdd, toDel);
            if (toAdd.isEmpty() && toDel.isEmpty()) {
                LOG.debug("runAfterCatalogReady nothing todo");
                continue;
            }

            Env.getCurrentSystemInfo().updateCloudBackends(toAdd, toDel);
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        Map<String, List<Backend>> clusterIdToBackend = Env.getCurrentSystemInfo().getCloudClusterIdToBackend();
        Set<String> allMysqlUserName = Env.getCurrentEnv().getAuth().getUserPrivTable().getAllMysqlUserName();
        if (allMysqlUserName != null) {
            //rpc to ms, to get mysql user can use cluster_id
            LOG.info("allMysqlUserName : {}", allMysqlUserName);
            // NOTE: rpc args all empty, use cluster_unique_id to get a instance's all cluster info.
            SelectdbCloud.GetClusterResponse response =
                    Env.getCurrentSystemInfo().getCloudCluster("", "", "");
            if (!response.hasStatus() || !response.getStatus().hasCode()
                    || (response.getStatus().getCode() != SelectdbCloud.MetaServiceCode.OK
                    && response.getStatus().getCode() != MetaServiceCode.CLUSTER_NOT_FOUND)) {
                LOG.warn("failed to get cloud cluster due to incomplete response, "
                        + "cloud_unique_id={}, response={}", Config.cloud_unique_id, response);
            } else {
                // use response's clusters info to generate a mysql_user_name to clusterPb map
                // mysqlUserName -> clusterPbs
                Map<String, List<ClusterPB>> userOwnedClusterMap = new HashMap<>();
                List<ClusterPB> computeClusters = response.getClusterList().stream()
                        .filter(c -> c.getType() != Type.SQL).collect(Collectors.toList());
                // root and admin user can access all clusters
                userOwnedClusterMap.put("root", computeClusters);
                // get meta service configured user name
                allMysqlUserName.stream().filter(user -> !user.equals("root")).forEach(user -> {
                    List<ClusterPB> userOwnedCluster = computeClusters.stream()
                            .filter(c -> c.getMysqlUserNameList().contains(user)).collect(Collectors.toList());
                    userOwnedClusterMap.put(user, userOwnedCluster);
                });
                // default user has the same cluster access rights as root user
                userOwnedClusterMap.put("admin", computeClusters);
                // selectdb cloud default users
                userOwnedClusterMap.put("selectdbAdmin", computeClusters);
                userOwnedClusterMap.put("selectdbView", computeClusters);
                // clusterId -> clusterPB
                Map<String, ClusterPB> remoteClusterIdToPB = new HashMap<>();
                userOwnedClusterMap.values().forEach(upbList ->
                        remoteClusterIdToPB.putAll(upbList.stream()
                            .collect(Collectors.toMap(ClusterPB::getClusterId, Function.identity())))
                );
                LOG.info("get cluster info by all mysql user name : {},userOwnedClusterMap: {}, clusterIds: {}",
                        allMysqlUserName, userOwnedClusterMap, remoteClusterIdToPB);
                Env.getCurrentSystemInfo().updateMysqlUserNameToClusterPb(userOwnedClusterMap);
                Set<String> localClusterIds = clusterIdToBackend.keySet();

                try {
                    // cluster_ids diff remote <clusterId, nodes> and local <clusterId, nodes>
                    // remote - local > 0, add bes to local
                    checkToAddCluster(remoteClusterIdToPB, localClusterIds);

                    // local - remote > 0, drop bes from local
                    checkToDelCluster(remoteClusterIdToPB, localClusterIds, clusterIdToBackend);

                    if (remoteClusterIdToPB.keySet().size() != clusterIdToBackend.keySet().size()) {
                        LOG.warn("impossible cluster id size not match, check it local {}, remote {}",
                                clusterIdToBackend, remoteClusterIdToPB);
                    }
                    // clusterID local == remote, diff nodes
                    checkDiffNode(remoteClusterIdToPB, clusterIdToBackend);

                    // check mem map
                    checkFeNodesMapValid();
                } catch (Exception e) {
                    LOG.warn("diff cluster has exception, {}", e.getMessage(), e);
                }
            }
        }

        // Metric
        clusterIdToBackend = Env.getCurrentSystemInfo().getCloudClusterIdToBackend();
        Map<String, String> clusterNameToId = Env.getCurrentSystemInfo().getCloudClusterNameToId();
        for (Map.Entry<String, String> entry : clusterNameToId.entrySet()) {
            long aliveNum = 0L;
            List<Backend> bes = clusterIdToBackend.get(entry.getValue());
            if (bes == null || bes.size() == 0) {
                LOG.info("cant get be nodes by cluster {}, bes {}", entry, bes);
                continue;
            }
            for (Backend backend : bes) {
                MetricRepo.CLOUD_CLUSTER_BACKEND_ALIVE.computeIfAbsent(backend.getAddress(), key -> {
                    GaugeMetricImpl<Integer> backendAlive = new GaugeMetricImpl<>("backend_alive", MetricUnit.NOUNIT,
                            "backend alive or not");
                    backendAlive.addLabel(new MetricLabel("cluster_id", entry.getValue()));
                    backendAlive.addLabel(new MetricLabel("cluster_name", entry.getKey()));
                    backendAlive.addLabel(new MetricLabel("address", key));
                    MetricRepo.DORIS_METRIC_REGISTER.addMetrics(backendAlive);
                    return backendAlive;
                }).setValue(backend.isAlive() ? 1 : 0);
                aliveNum = backend.isAlive() ? aliveNum + 1 : aliveNum;
            }

            MetricRepo.CLOUD_CLUSTER_BACKEND_ALIVE_TOTAL.computeIfAbsent(entry.getKey(), key -> {
                GaugeMetricImpl<Long> backendAliveTotal = new GaugeMetricImpl<>("backend_alive_total",
                        MetricUnit.NOUNIT,
                        "backend alive num in cluster");
                backendAliveTotal.addLabel(new MetricLabel("cluster_id", entry.getValue()));
                backendAliveTotal.addLabel(new MetricLabel("cluster_name", key));
                MetricRepo.DORIS_METRIC_REGISTER.addMetrics(backendAliveTotal);
                return backendAliveTotal;
            }).setValue(aliveNum);
        }

        LOG.info("daemon cluster get cluster info succ, current cloudClusterIdToBackendMap: {}",
                Env.getCurrentSystemInfo().getCloudClusterIdToBackend());
        getObserverFes();
    }

    private void checkFeNodesMapValid() {
        LOG.debug("begin checkFeNodesMapValid");
        Map<String, List<Backend>> clusterIdToBackend = Env.getCurrentSystemInfo().getCloudClusterIdToBackend();
        Set<String> clusterIds = new HashSet<>();
        Set<String> clusterNames = new HashSet<>();
        clusterIdToBackend.forEach((clusterId, bes) -> {
            if (bes.isEmpty()) {
                LOG.warn("impossible, somewhere err, clusterId {}, clusterIdToBeMap {}", clusterId, clusterIdToBackend);
                clusterIdToBackend.remove(clusterId);
            }
            bes.forEach(be -> {
                clusterIds.add(be.getCloudClusterId());
                clusterNames.add(be.getCloudClusterName());
            });
        });

        Map<String, String> nameToId = Env.getCurrentSystemInfo().getCloudClusterNameToId();
        nameToId.forEach((clusterName, clusterId) -> {
            if (!clusterIdToBackend.containsKey(clusterId)) {
                LOG.warn("impossible, somewhere err, clusterId {}, clusterName {}, clusterNameToIdMap {}",
                        clusterId, clusterName, nameToId);
                nameToId.remove(clusterName);
            }
        });

        if (!clusterNames.containsAll(nameToId.keySet()) || !nameToId.keySet().containsAll(clusterNames)) {
            LOG.warn("impossible, somewhere err, clusterNames {}, nameToId {}", clusterNames, nameToId);
        }
        if (!clusterIds.containsAll(nameToId.values()) || !nameToId.values().containsAll(clusterIds)) {
            LOG.warn("impossible, somewhere err, clusterIds {}, nameToId {}", clusterIds, nameToId);
        }
        if (!clusterIds.containsAll(clusterIdToBackend.keySet())
                || !clusterIdToBackend.keySet().containsAll(clusterIds)) {
            LOG.warn("impossible, somewhere err, clusterIds {}, clusterIdToBackend {}",
                    clusterIds, clusterIdToBackend);
        }
    }

    private void getObserverFes() {
        SelectdbCloud.GetClusterResponse response =
                Env.getCurrentSystemInfo()
                    .getCloudCluster(Config.cloud_sql_server_cluster_name, Config.cloud_sql_server_cluster_id, "");
        if (!response.hasStatus() || !response.getStatus().hasCode()
                || response.getStatus().getCode() != SelectdbCloud.MetaServiceCode.OK) {
            LOG.warn("failed to get cloud cluster due to incomplete response, "
                    + "cloud_unique_id={}, clusterId={}, response={}",
                    Config.cloud_unique_id, Config.cloud_sql_server_cluster_id, response);
            return;
        }
        // Note: get_cluster interface cluster(option -> repeated), so it has at least one cluster.
        if (response.getClusterCount() == 0) {
            LOG.warn("meta service error , return cluster zero, plz check it, "
                    + "cloud_unique_id={}, clusterId={}, response={}",
                    Config.cloud_unique_id, Config.cloud_sql_server_cluster_id, response);
            return;
        }

        ClusterPB cpb = response.getCluster(0);
        LOG.debug("get cloud cluster, clusterId={} nodes={}",
                Config.cloud_sql_server_cluster_id, cpb.getNodesList());
        List<Frontend> currentFes = Env.getCurrentEnv().getFrontends(FrontendNodeType.OBSERVER);
        List<Frontend> toAdd = new ArrayList<>();
        List<Frontend> toDel = new ArrayList<>();
        List<SelectdbCloud.NodeInfoPB> expectedFes = cpb.getNodesList();
        diffNodes(toAdd, toDel, () -> {
            Map<String, Frontend> currentMap = new HashMap<>();
            String selfNode = Env.getCurrentEnv().getSelfNode().toString();
            for (Frontend fe : currentFes) {
                String endpoint = fe.getHost() + ":" + fe.getEditLogPort();
                if (selfNode.equals(endpoint)) {
                    continue;
                }
                currentMap.put(endpoint, fe);
            }
            return currentMap;
        }, () -> {
            Map<String, Frontend> nodeMap = new HashMap<>();
            String selfNode = Env.getCurrentEnv().getSelfNode().toString();
            for (SelectdbCloud.NodeInfoPB node : expectedFes) {
                String endpoint = node.getIp() + ":" + node.getEditLogPort();
                if (selfNode.equals(endpoint)) {
                    continue;
                }
                Frontend fe = new Frontend(FrontendNodeType.OBSERVER,
                        Env.genFeNodeNameFromMeta(node.getIp(), node.getEditLogPort(),
                        node.getCtime() * 1000), node.getIp(), node.getEditLogPort());
                nodeMap.put(endpoint, fe);
            }
            return nodeMap;
        });
        LOG.info("diffFrontends nodes: {}, current: {}, toAdd: {}, toDel: {}",
                expectedFes, currentFes, toAdd, toDel);
        if (toAdd.isEmpty() && toDel.isEmpty()) {
            LOG.debug("runAfterCatalogReady getObserverFes nothing todo");
            return;
        }
        try {
            Env.getCurrentSystemInfo().updateCloudFrontends(toAdd, toDel);
        } catch (DdlException e) {
            LOG.warn("update cloud frontends exception e: {}, msg: {}", e, e.getMessage());
        }
    }
}
