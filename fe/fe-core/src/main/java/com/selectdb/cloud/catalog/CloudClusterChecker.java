package com.selectdb.cloud.catalog;

import com.selectdb.cloud.proto.SelectdbCloud;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.system.Backend;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CloudClusterChecker extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CloudClusterChecker.class);

    public CloudClusterChecker() {
        super("cloud cluster be check", FeConstants.cloud_cluster_check_interval_second * 1000);
    }


    /**
     * Diff 2 collections of current and the dest.
     *
     * @param destState expected backend state
     * @param currentState current backend state
     * @param toAdd destState - curentState
     * @param toDel currentState - destState
     */
    private void diffBackends(List<SelectdbCloud.NodeInfoPB> destState, List<Backend> currentState,
                                            List<Backend> toAdd, List<Backend> toDel) {
        if (toAdd == null || toDel == null) {
            return;
        }

        // TODO(gavin): Consider VPC
        // vpc:ip:port -> Backend
        Map<String, Backend> currentMap = new HashMap<>();
        for (Backend be : currentState) {
            String endpoint = be.getHost() + ":" + be.getHeartbeatPort();
            currentMap.put(endpoint, be);
        }

        Map<String, Backend> nodeMap = new HashMap<>();
        for (SelectdbCloud.NodeInfoPB node : destState) {
            String endpoint = node.getIp() + ":" + node.getHeartbeatPort();
            Backend b = new Backend(Env.getCurrentEnv().getNextId(), node.getIp(), node.getHeartbeatPort());
            nodeMap.put(endpoint, b);
        }

        toDel.addAll(currentMap.keySet().stream().filter(i -> !nodeMap.containsKey(i))
                .map(i -> currentMap.get(i)).collect(Collectors.toList()));

        toAdd.addAll(nodeMap.keySet().stream().filter(i -> !currentMap.containsKey(i))
                .map(i -> nodeMap.get(i)).collect(Collectors.toList()));

        LOG.debug("diffBackends nodes: {}, current: {}, needAdd: {}, needDel: {}",
                destState, currentState, toAdd, toDel);
    }

    @Override
    protected void runAfterCatalogReady() {
        // TODO(gavin): resolve data race with `SystemInfoService.addCloudCluster()`
        Map<String, List<Backend>> clusterIdToBackend =
                Env.getCurrentSystemInfo().getCloudClusterIdToBackend();
        for (String clusterName : clusterIdToBackend.keySet()) {
            SelectdbCloud.GetClusterResponse response =
                    Env.getCurrentSystemInfo().getCloudCluster(Config.cloud_unique_id, clusterName);
            if (!response.hasStatus() || !response.getStatus().hasCode()
                    || response.getStatus().getCode() != SelectdbCloud.MetaServiceCode.OK) {
                LOG.warn("failed to get cloud cluster due to incomplete response, "
                        + "cloud_unique_id={}, clusterName={}, response={}",
                        Config.cloud_unique_id, clusterName, response);
                continue;
            }
            List<Backend> currentClusterBes = clusterIdToBackend.get(clusterName);
            List<Backend> toAdd = new ArrayList<>();
            List<Backend> toDel = new ArrayList<>();
            diffBackends(response.getCluster().getComputeNodeList(), currentClusterBes, toAdd, toDel);
            if (toAdd.isEmpty() && toDel.isEmpty()) {
                LOG.debug("runAfterCatalogReady nothing todo");
                continue;
            }
            Env.getCurrentSystemInfo().updateCloudBackends(toAdd, toDel);
        }

        LOG.debug("daemon cluster get cluster info succ, current uniqueIdToClusterNameMap: {}",
                Env.getCurrentSystemInfo().getCloudClusterIdToBackend());
    }
}
