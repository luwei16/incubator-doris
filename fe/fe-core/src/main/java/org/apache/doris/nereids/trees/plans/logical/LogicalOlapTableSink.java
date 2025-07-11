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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.PropagateFuncDeps;
import org.apache.doris.nereids.trees.plans.algebra.Sink;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * logical olap table sink for insert command
 */
public class LogicalOlapTableSink<CHILD_TYPE extends Plan> extends LogicalTableSink<CHILD_TYPE>
        implements Sink, PropagateFuncDeps {
    // bound data sink
    private final Database database;
    private final OlapTable targetTable;
    private final List<Long> partitionIds;
    private final boolean isPartialUpdate;
    private final TPartialUpdateNewRowPolicy partialUpdateNewKeyPolicy;
    private final DMLCommandType dmlCommandType;
    private final List<Expression> partitionExprList;
    private final Map<Long, Expression> syncMvWhereClauses;
    private final List<Slot> targetTableSlots;

    public LogicalOlapTableSink(Database database, OlapTable targetTable, List<Column> cols, List<Long> partitionIds,
            List<NamedExpression> outputExprs, boolean isPartialUpdate,
            TPartialUpdateNewRowPolicy partialUpdateNewKeyPolicy,
            DMLCommandType dmlCommandType, CHILD_TYPE child) {
        this(database, targetTable, cols, partitionIds, outputExprs, isPartialUpdate, partialUpdateNewKeyPolicy,
                dmlCommandType, Optional.empty(), Optional.empty(), child);
    }

    /**
     * constructor
     */
    public LogicalOlapTableSink(Database database, OlapTable targetTable, List<Column> cols,
            List<Long> partitionIds, List<NamedExpression> outputExprs, boolean isPartialUpdate,
            TPartialUpdateNewRowPolicy partialUpdateNewKeyPolicy,
            DMLCommandType dmlCommandType, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        this(database, targetTable, cols, partitionIds, outputExprs, isPartialUpdate, partialUpdateNewKeyPolicy,
                dmlCommandType, new ArrayList<>(), new HashMap<>(), new ArrayList<>(),
                groupExpression, logicalProperties, child);
    }

    private LogicalOlapTableSink(Database database, OlapTable targetTable, List<Column> cols,
            List<Long> partitionIds, List<NamedExpression> outputExprs, boolean isPartialUpdate,
            TPartialUpdateNewRowPolicy partialUpdateNewKeyPolicy, DMLCommandType dmlCommandType,
            List<Expression> partitionExprList, Map<Long, Expression> syncMvWhereClauses,
            List<Slot> targetTableSlots, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_OLAP_TABLE_SINK, outputExprs, groupExpression, logicalProperties, cols, child);
        this.database = Objects.requireNonNull(database, "database != null in LogicalOlapTableSink");
        this.targetTable = Objects.requireNonNull(targetTable, "targetTable != null in LogicalOlapTableSink");
        this.isPartialUpdate = isPartialUpdate;
        this.partialUpdateNewKeyPolicy = partialUpdateNewKeyPolicy;
        this.dmlCommandType = dmlCommandType;
        this.partitionIds = Utils.copyRequiredList(partitionIds);
        this.partitionExprList = partitionExprList;
        this.syncMvWhereClauses = syncMvWhereClauses;
        this.targetTableSlots = targetTableSlots;
    }

    /**
     * withChildAndUpdateOutput
     */
    public Plan withChildAndUpdateOutput(Plan child, List<Expression> partitionExprList,
            Map<Long, Expression> syncMvWhereClauses, List<Slot> targetTableSlots) {
        List<NamedExpression> output = child.getOutput().stream().map(NamedExpression.class::cast)
                .collect(ImmutableList.toImmutableList());
        return new LogicalOlapTableSink<>(database, targetTable, cols, partitionIds, output,
                isPartialUpdate, partialUpdateNewKeyPolicy, dmlCommandType, partitionExprList, syncMvWhereClauses,
                targetTableSlots, Optional.empty(), Optional.empty(), child);
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1, "LogicalOlapTableSink only accepts one child");
        return new LogicalOlapTableSink<>(database, targetTable, cols, partitionIds, outputExprs,
                isPartialUpdate, partialUpdateNewKeyPolicy, dmlCommandType, partitionExprList, syncMvWhereClauses,
                targetTableSlots, Optional.empty(), Optional.empty(), children.get(0));
    }

    public Database getDatabase() {
        return database;
    }

    public OlapTable getTargetTable() {
        return targetTable;
    }

    public List<Long> getPartitionIds() {
        return partitionIds;
    }

    public boolean isPartialUpdate() {
        return isPartialUpdate;
    }

    public TPartialUpdateNewRowPolicy getPartialUpdateNewRowPolicy() {
        return partialUpdateNewKeyPolicy;
    }

    public DMLCommandType getDmlCommandType() {
        return dmlCommandType;
    }

    public List<Expression> getPartitionExprList() {
        return partitionExprList;
    }

    public Map<Long, Expression> getSyncMvWhereClauses() {
        return syncMvWhereClauses;
    }

    public List<Slot> getTargetTableSlots() {
        return targetTableSlots;
    }

    public LogicalOlapTableSink<CHILD_TYPE> withOutputExprs(List<NamedExpression> outputExprs) {
        return new LogicalOlapTableSink<>(database, targetTable, cols, partitionIds, outputExprs,
                isPartialUpdate, partialUpdateNewKeyPolicy, dmlCommandType, partitionExprList, syncMvWhereClauses,
                targetTableSlots, Optional.empty(), Optional.empty(), child());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        LogicalOlapTableSink<?> that = (LogicalOlapTableSink<?>) o;
        return isPartialUpdate == that.isPartialUpdate && dmlCommandType == that.dmlCommandType
                && partialUpdateNewKeyPolicy == that.partialUpdateNewKeyPolicy
                && Objects.equals(database, that.database)
                && Objects.equals(targetTable, that.targetTable) && Objects.equals(cols, that.cols)
                && Objects.equals(partitionIds, that.partitionIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), database, targetTable, cols, partitionIds,
                isPartialUpdate, partialUpdateNewKeyPolicy, dmlCommandType);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalOlapTableSink[" + id.asInt() + "]",
                "outputExprs", outputExprs,
                "database", database.getFullName(),
                "targetTable", targetTable.getName(),
                "cols", cols,
                "partitionIds", partitionIds,
                "isPartialUpdate", isPartialUpdate,
                "partialUpdateNewKeyPolicy", partialUpdateNewKeyPolicy,
                "dmlCommandType", dmlCommandType
        );
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalOlapTableSink(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalOlapTableSink<>(database, targetTable, cols, partitionIds, outputExprs,
                isPartialUpdate, partialUpdateNewKeyPolicy, dmlCommandType, partitionExprList, syncMvWhereClauses,
                targetTableSlots, groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalOlapTableSink<>(database, targetTable, cols, partitionIds, outputExprs,
                isPartialUpdate, partialUpdateNewKeyPolicy, dmlCommandType, partitionExprList, syncMvWhereClauses,
                targetTableSlots, groupExpression, logicalProperties, children.get(0));
    }

    public Plan withPartitionExprAndMvWhereClause(List<Expression> partitionExprList,
                                                  Map<Long, Expression> syncMvWhereClauses) {
        return new LogicalOlapTableSink<>(database, targetTable, cols, partitionIds, outputExprs,
                isPartialUpdate, partialUpdateNewKeyPolicy, dmlCommandType, partitionExprList, syncMvWhereClauses,
                targetTableSlots, Optional.empty(), Optional.empty(), child());
    }
}
