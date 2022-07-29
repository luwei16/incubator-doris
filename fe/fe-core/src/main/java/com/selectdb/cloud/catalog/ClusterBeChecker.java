package com.selectdb.cloud.catalog;

import com.selectdb.cloud.proto.SelectdbCloud;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.system.Backend;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ClusterBeChecker extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(ClusterBeChecker.class);

    public ClusterBeChecker() {
        super("cloud cluster be check", FeConstants.cloud_cluster_check_interval_second * 1000);
    }


    private void backendsDiffWithCurrent(List<SelectdbCloud.NodeInfoPB> nodes, List<Backend> currentBes,
                                            List<Backend> needAdd, List<Backend> needDel) {
        if (needAdd == null || needDel == null) {
            return;
        }
        Map<String, Backend> currentMap = new HashMap<>();
        for (Backend currentBe : currentBes) {
            String endpoint = currentBe.getHost() + ":" + currentBe.getHeartbeatPort();
            currentMap.put(endpoint, currentBe);
        }

        Map<String, Backend> nodeMap = new HashMap<>();
        for (SelectdbCloud.NodeInfoPB node : nodes) {
            String endpoint = node.getIp() + ":" + node.getHeartbeatPort();
            Backend b = new Backend(Catalog.getCurrentCatalog().getNextId(), node.getIp(), node.getHeartbeatPort());
            nodeMap.put(endpoint, b);
        }

        for (String current : currentMap.keySet()) {
            if (!nodeMap.containsKey(current)) {
                needDel.add(currentMap.get(current));
            }
        }

        for (String node : nodeMap.keySet()) {
            if (!currentMap.containsKey(node)) {
                Backend backend = nodeMap.get(node);
                backend.setId(Catalog.getCurrentCatalog().getNextId());
                needAdd.add(backend);
            }
        }
        LOG.debug("backendsDiffWithCurrent nodes: {}, currentBes: {}, needAdd: {}, needDel: {}",
                nodes, currentBes, needAdd, needDel);
    }

    @Override
    protected void runAfterCatalogReady() {
        Map<String, List<Backend>> copiedClusterNameToBackendRef =
                Catalog.getCurrentSystemInfo().copiedClusterNameToBackendRef();
        for (String clusterName : copiedClusterNameToBackendRef.keySet()) {
            SelectdbCloud.GetClusterResponse response = Catalog.getCurrentSystemInfo()
                    .rpcToMetaGetClusterInfo(Catalog.getCurrentSystemInfo().cloudUniqueId, clusterName);
            if (response.hasStatus() && response.getStatus().hasCode() && response.getStatus().getCode() == 0) {
                List<Backend> currentClusterBes = copiedClusterNameToBackendRef.get(clusterName);
                List<Backend> needAdd = new ArrayList<>();
                List<Backend> needDel = new ArrayList<>();
                backendsDiffWithCurrent(new ArrayList<>(response.getCluster().getComputeNodeList()),
                        currentClusterBes, needAdd, needDel);
                if (needAdd.size() == 0 && needDel.size() == 0) {
                    // do nothing
                    LOG.debug("runAfterCatalogReady nothing todo");
                    continue;
                }
                if (needAdd.size() != 0) {
                    List<Backend> statusOkBackends = Catalog.getCurrentHeartbeatMgr().checkBeStatus(needAdd);
                    List<Long> backendIds = Catalog.getCurrentSystemInfo().getBackendIds(false);
                    // heart beat
                    for (Backend statusOkBackend : statusOkBackends) {
                        if (!backendIds.contains(statusOkBackend.getId())) {
                            Catalog.getCurrentSystemInfo().addBackend(statusOkBackend);
                            Catalog.getCurrentSystemInfo().addClusterNameToBackendRef(statusOkBackend);
                            Catalog.getCurrentSystemInfo().addClusterIdToBackendRef(statusOkBackend);
                        }
                    }
                }
                if (needDel.size() != 0) {
                    Set<Long> toDel = needDel.stream().map(Backend::getId).collect(Collectors.toSet());
                    List<Backend> afterRemove = currentClusterBes.stream()
                            .filter(backend -> !toDel.contains(backend.getId())).collect(Collectors.toList());
                    Catalog.getCurrentSystemInfo().updateClusterNameToBackendRefBackends(clusterName, afterRemove);
                    Catalog.getCurrentSystemInfo().updateClusterIdToBackendRefBackends(clusterName, afterRemove);
                }

            } else {
                LOG.warn("daemon cluster mgr get cluster info err. {}, {}, {}",
                        Catalog.getCurrentSystemInfo().cloudUniqueId, clusterName, response);
            }
        }

        LOG.debug("daemon cluster get cluster info succ, current uniqueIdToClusterNameMap: {}",
                Catalog.getCurrentSystemInfo().copiedClusterNameToBackendRef());
    }
}
