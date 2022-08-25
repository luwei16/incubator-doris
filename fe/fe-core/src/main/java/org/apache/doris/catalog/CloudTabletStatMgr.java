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

import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TTabletStat;
import org.apache.doris.thrift.TTabletStatResult;

import com.google.common.collect.ImmutableMap;
import com.selectdb.cloud.proto.SelectdbCloud;
import com.selectdb.cloud.rpc.MetaServiceProxy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;


/*
 * CloudTabletStatMgr is for collecting tablet(replica) statistics from backends.
 * Each FE will collect by itself.
 */
public class CloudTabletStatMgr extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CloudTabletStatMgr.class);

    private ForkJoinPool taskPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());

    public CloudTabletStatMgr() {
        super("cloud tablet stat mgr", Config.tablet_stat_update_interval_second * 1000);
    }

    @Override
    protected void runAfterCatalogReady() {
        LOG.info("lw test cloud tablet stat begin");
        long start = System.currentTimeMillis();
        List<SelectdbCloud.GetTabletStatsRequest> reqList =
                new ArrayList<SelectdbCloud.GetTabletStatsRequest>();

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

                table.readLock();
                try {
                    OlapTable tbl = (OlapTable) table;
                    for (Partition partition : tbl.getAllPartitions()) {
                        SelectdbCloud.GetTabletStatsRequest.Builder builder =
                                SelectdbCloud.GetTabletStatsRequest.newBuilder();
                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            for (Long tabletId : index.getTabletIdsInOrder()) {
                                Tablet tablet = index.getTablet(tabletId);

                                SelectdbCloud.TabletIndexPB.Builder indexBuilder =
                                        SelectdbCloud.TabletIndexPB.newBuilder();
                                indexBuilder.setDbId(dbId);
                                indexBuilder.setTableId(table.getId());
                                indexBuilder.setIndexId(index.getId());
                                indexBuilder.setPartitionId(partition.getId());
                                indexBuilder.setTabletId(tablet.getId());
                                builder.addTabletIdx(indexBuilder);
                            }
                        }
                        SelectdbCloud.GetTabletStatsRequest getTabletReq = builder.build();
                        reqList.add(getTabletReq);
                    } // partitions
                } finally {
                    table.readUnlock();
                }
            } // tables
        } // end for dbs

        for (SelectdbCloud.GetTabletStatsRequest req : reqList) {
            SelectdbCloud.GetTabletStatsResponse resp;
            try {
                resp = getTabletStats(req);
            } catch (RpcException e) {
                LOG.info("lw test get exception {} ", e);
                continue;
            }
            LOG.info("lw test get rpc resp msg : {} ", resp.getStatus().getMsg());

            if (resp.getStatus().getCode() != SelectdbCloud.MetaServiceCode.OK) {
                continue;
            }

            updateTabletStat(resp);
        }

        LOG.debug("finished to get tablet stat of all backends. cost: {} ms",
                (System.currentTimeMillis() - start));

        // after update replica in all backends, update index row num
        start = System.currentTimeMillis();
        //List<Long> dbIds = Env.getCurrentInternalCatalog().getDbIds();
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
                        long version = partition.getVisibleVersion();
                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            long indexRowCount = 0L;
                            for (Tablet tablet : index.getTablets()) {
                                long tabletRowCount = 0L;
                                for (Replica replica : tablet.getReplicas()) {
                                    if (replica.checkVersionCatchUp(version, false)
                                            && replica.getRowCount() > tabletRowCount) {
                                        tabletRowCount = replica.getRowCount();
                                    }
                                }
                                indexRowCount += tabletRowCount;
                            } // end for tablets
                            index.setRowCount(indexRowCount);
                        } // end for indices
                    } // end for partitions
                    LOG.debug("finished to set row num for table: {} in database: {}",
                             table.getName(), db.getFullName());
                } finally {
                    table.writeUnlock();
                }
            }
        }
        LOG.info("finished to update index row num of all databases. cost: {} ms",
                (System.currentTimeMillis() - start));
    }

    private void updateTabletStat(SelectdbCloud.GetTabletStatsResponse result) {
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        for (SelectdbCloud.TabletStatsPB stat : result.getTabletStatsList()) {
            if (invertedIndex.getTabletMeta(stat.getIdx().getTabletId()) != null) {
                List<Replica> replicas = invertedIndex.getReplicasByTabletId(stat.getIdx().getTabletId());
                if (replicas != null && !replicas.isEmpty() && replicas.get(0) != null) {
                    replicas.get(0).updateStat(stat.getDataSize(), 0, stat.getNumRows(), -1);
                }
            }
        }
    }

    private SelectdbCloud.GetTabletStatsResponse getTabletStats(SelectdbCloud.GetTabletStatsRequest req)
            throws RpcException {
        String metaEndPoint = Config.meta_service_endpoint;
        String[] splitMetaEndPoint = metaEndPoint.split(":");
        TNetworkAddress metaAddress =
                new TNetworkAddress(splitMetaEndPoint[0], Integer.parseInt(splitMetaEndPoint[1]));

        SelectdbCloud.GetTabletStatsResponse response;
        try {
            response = MetaServiceProxy.getInstance().getTabletStats(metaAddress, req);
        } catch (RpcException e) {
            LOG.info("lw test get exception {}", e);
            throw e;
        }

        return response;
    }
}
