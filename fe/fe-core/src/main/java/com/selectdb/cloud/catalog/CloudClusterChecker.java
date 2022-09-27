package com.selectdb.cloud.catalog;

import com.selectdb.cloud.proto.SelectdbCloud;

import org.apache.doris.catalog.Env;
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
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class CloudClusterChecker extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CloudClusterChecker.class);

    public CloudClusterChecker() {
        super("cloud cluster check", FeConstants.cloud_cluster_check_interval_second * 1000);
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

    @Override
    protected void runAfterCatalogReady() {
        Map<String, List<Backend>> clusterIdToBackend = Env.getCurrentSystemInfo().getCloudClusterIdToBackend();
        for (String clusterId : clusterIdToBackend.keySet()) {
            SelectdbCloud.GetClusterResponse response =
                    Env.getCurrentSystemInfo().getCloudCluster("", clusterId, "");
            if (!response.hasStatus() || !response.getStatus().hasCode()
                    || response.getStatus().getCode() != SelectdbCloud.MetaServiceCode.OK) {
                LOG.warn("failed to get cloud cluster due to incomplete response, "
                        + "cloud_unique_id={}, clusterId={}, response={}",
                        Config.cloud_unique_id, clusterId, response);
                continue;
            }
            LOG.info("get cloud cluster, clusterId={} nodes={}", clusterId, response.getCluster().getNodesList());
            List<Backend> currentBes = clusterIdToBackend.get(clusterId);
            String currentClusterName = currentBes.stream().map(Backend::getCloudClusterName).findFirst().orElse("");
            String newClusterName = response.getCluster().getClusterName();
            if (!newClusterName.equals(currentClusterName)) {
                // rename cluster's name
                LOG.info("cluster_name corresponding to cluster_id has been changed,"
                        + " cluster_id : {} , current_cluster_name : {}, new_cluster_name :{}",
                        clusterId, currentClusterName, newClusterName);
                // change all be's cluster_name
                currentBes.forEach(b -> b.setCloudClusterName(newClusterName));
                // update clusterNameToId
                Env.getCurrentSystemInfo().updateClusterNameToId(newClusterName, currentClusterName, clusterId);
            }
            List<Backend> toAdd = new ArrayList<>();
            List<Backend> toDel = new ArrayList<>();
            List<SelectdbCloud.NodeInfoPB> expectedBes = response.getCluster().getNodesList();
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
                    Backend b = new Backend(Env.getCurrentEnv().getNextId(),
                            node.getIp(), node.getHeartbeatPort());
                    nodeMap.put(endpoint, b);
                }
                return nodeMap;
            });
            LOG.debug("diffBackends nodes: {}, current: {}, toAdd: {}, toDel: {}",
                    expectedBes, currentBes, toAdd, toDel);
            if (toAdd.isEmpty() && toDel.isEmpty()) {
                LOG.debug("runAfterCatalogReady nothing todo");
                continue;
            }

            // Attach tag to BEs
            Map<String, String> newTagMap = Tag.DEFAULT_BACKEND_TAG.toMap();
            newTagMap.put(Tag.CLOUD_CLUSTER_NAME, response.getCluster().getClusterName());
            newTagMap.put(Tag.CLOUD_CLUSTER_ID, response.getCluster().getClusterId());
            toAdd.stream().forEach(i -> i.setTagMap(newTagMap));
            Env.getCurrentSystemInfo().updateCloudBackends(toAdd, toDel);
        }

        clusterIdToBackend = Env.getCurrentSystemInfo().getCloudClusterIdToBackend();
        for (Map.Entry<String, List<Backend>> entry : clusterIdToBackend.entrySet()) {
            Long aliveNum = 0L;
            for (Backend backend : entry.getValue()) {
                MetricRepo.CLOUD_CLUSTER_BACKEND_ALIVE.computeIfAbsent(backend.getAddress(), key -> {
                    GaugeMetricImpl<Boolean> backendAlive = new GaugeMetricImpl<>("backend_alive", MetricUnit.NOUNIT,
                            "backend alive or not");
                    backendAlive.addLabel(new MetricLabel("cluster", entry.getKey()));
                    backendAlive.addLabel(new MetricLabel("address", key));
                    MetricRepo.DORIS_METRIC_REGISTER.addMetrics(backendAlive);
                    return backendAlive;
                }).setValue(backend.isAlive());
                aliveNum = backend.isAlive() ? aliveNum + 1 : aliveNum;
            }
            MetricRepo.CLOUD_CLUSTER_BACKEND_ALIVE_TOTAL.computeIfAbsent(entry.getKey(), key -> {
                GaugeMetricImpl<Long> backendAliveTotal =
                                        new GaugeMetricImpl<>("backend_alive_total", MetricUnit.NOUNIT,
                            "backend alive num in cluster");
                backendAliveTotal.addLabel(new MetricLabel("cluster", key));
                MetricRepo.DORIS_METRIC_REGISTER.addMetrics(backendAliveTotal);
                return backendAliveTotal;
            }).setValue(aliveNum);
        }
        LOG.debug("daemon cluster get cluster info succ, current cloudClusterIdToBackendMap: {}",
                Env.getCurrentSystemInfo().getCloudClusterIdToBackend());
        getObserverFes();
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
        LOG.debug("get cloud cluster, clusterId={} nodes={}",
                Config.cloud_sql_server_cluster_id, response.getCluster().getNodesList());
        List<Frontend> currentFes = Env.getCurrentEnv().getFrontends(FrontendNodeType.OBSERVER);
        List<Frontend> toAdd = new ArrayList<>();
        List<Frontend> toDel = new ArrayList<>();
        List<SelectdbCloud.NodeInfoPB> expectedFes = response.getCluster().getNodesList();
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
        LOG.debug("diffFrontends nodes: {}, current: {}, toAdd: {}, toDel: {}",
                expectedFes, currentFes, toAdd, toDel);
        if (toAdd.isEmpty() && toDel.isEmpty()) {
            LOG.debug("runAfterCatalogReady getObserverFes nothing todo");
            return;
        }
        try {
            Env.getCurrentSystemInfo().updateCloudFrontends(toAdd, toDel);
        } catch (DdlException e) {
            LOG.warn("update cloud fronteds exception e: {}, msg: {}", e, e.getMessage());
        }
    }
}
