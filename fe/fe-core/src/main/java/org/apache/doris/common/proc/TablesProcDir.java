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

package org.apache.doris.common.proc;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.rpc.RpcException;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/*
 * SHOW PROC /dbs/dbId/
 * show table family groups' info within a db
 */
public class TablesProcDir implements ProcDirInterface {
    private static final Logger LOG = LogManager.getLogger(ProcDirInterface.class);
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TableId").add("TableName").add("IndexNum").add("PartitionColumnName")
            .add("PartitionNum").add("State").add("Type").add("LastConsistencyCheckTime").add("ReplicaCount")
            .add("VisibleVersion").add("VisibleVersionTime").add("RunningTransactionNum").add("LastUpdateTime")
            .build();

    private DatabaseIf db;

    public TablesProcDir(DatabaseIf db) {
        this.db = db;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String tableIdStr) throws AnalysisException {
        Preconditions.checkNotNull(db);
        if (Strings.isNullOrEmpty(tableIdStr)) {
            throw new AnalysisException("TableIdStr is null");
        }

        long tableId;
        try {
            tableId = Long.parseLong(tableIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid table id format: " + tableIdStr);
        }

        TableIf table = db.getTableOrAnalysisException(tableId);
        return new TableProcDir(db, table);
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(db);

        // get info
        List<List<Comparable>> tableInfos = new ArrayList<List<Comparable>>();
        List<TableIf> tableList = db.getTables();
        Map<Long, List<Long>> transToTableListInfo = Env.getCurrentGlobalTransactionMgr()
                .getDbRunningTransInfo(db.getId());
        Map<Long, Integer> tableToTransNumInfo = Maps.newHashMap();
        for (Entry<Long, List<Long>> transWithTableList : transToTableListInfo.entrySet()) {
            for (Long tableId : transWithTableList.getValue()) {
                int transNum = tableToTransNumInfo.getOrDefault(tableId, 0);
                transNum++;
                tableToTransNumInfo.put(tableId, transNum);
            }
        }
        for (TableIf table : tableList) {
            List<Comparable> tableInfo = new ArrayList<Comparable>();
            int partitionNum = 1;
            long replicaCount = 0;
            String partitionKey = FeConstants.null_string;
            table.readLock();
            try {
                if (table instanceof OlapTable) {
                    OlapTable olapTable = (OlapTable) table;
                    if (olapTable.getPartitionInfo().getType() == PartitionType.RANGE) {
                        partitionNum = olapTable.getPartitions().size();
                        RangePartitionInfo info = (RangePartitionInfo) olapTable.getPartitionInfo();
                        partitionKey = "";
                        int idx = 0;
                        for (Column column : info.getPartitionColumns()) {
                            if (idx != 0) {
                                partitionKey += ", ";
                            }
                            partitionKey += column.getName();
                            ++idx;
                        }
                    }
                    long version = 0;
                    try {
                        version = ((OlapTable) table).getVisibleVersion();
                    } catch (RpcException e) {
                        LOG.warn("table {}, in cloud getVisibleVersion exception", table.getName(), e);
                        throw new AnalysisException(e.getMessage());
                    }
                    replicaCount = olapTable.getReplicaCount();
                    tableInfo.add(table.getId());
                    tableInfo.add(table.getName());
                    tableInfo.add(olapTable.getIndexNameToId().size());
                    tableInfo.add(partitionKey);
                    tableInfo.add(partitionNum);
                    tableInfo.add(olapTable.getState());
                    tableInfo.add(table.getType());
                    // last check time
                    tableInfo.add(TimeUtils.longToTimeString(olapTable.getLastCheckTime()));
                    tableInfo.add(replicaCount);
                    tableInfo.add(version);
                    tableInfo.add(olapTable.getVisibleVersionTime());
                } else {
                    tableInfo.add(table.getId());
                    tableInfo.add(table.getName());
                    tableInfo.add(FeConstants.null_string);
                    tableInfo.add(partitionKey);
                    tableInfo.add(partitionNum);
                    tableInfo.add(FeConstants.null_string);
                    tableInfo.add(table.getType());
                    // last check time
                    tableInfo.add(FeConstants.null_string);
                    tableInfo.add(replicaCount);
                    tableInfo.add(FeConstants.null_string);
                    tableInfo.add(FeConstants.null_string);
                }
                int runningTransactionNum = tableToTransNumInfo.getOrDefault(table.getId(), 0);
                tableInfo.add(runningTransactionNum);
                tableInfo.add(TimeUtils.longToTimeString(table.getUpdateTime()));
                tableInfos.add(tableInfo);
            } finally {
                table.readUnlock();
            }
        }

        // sort by table id
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0);
        Collections.sort(tableInfos, comparator);

        // set result
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        for (List<Comparable> info : tableInfos) {
            List<String> row = new ArrayList<String>(info.size());
            for (Comparable comparable : info) {
                row.add(comparable.toString());
            }
            result.addRow(row);
        }

        return result;
    }
}
