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

package org.apache.doris.transaction;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.QuotaExceedException;
import org.apache.doris.common.UserException;
import org.apache.doris.persist.BatchRemoveTransactionsOperation;
import org.apache.doris.persist.EditLog;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TWaitingTxnStatusRequest;
import org.apache.doris.thrift.TWaitingTxnStatusResult;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;

import com.google.common.base.Preconditions;
import com.selectdb.cloud.proto.SelectdbCloud.BeginTxnRequest;
import com.selectdb.cloud.proto.SelectdbCloud.BeginTxnResponse;
import com.selectdb.cloud.proto.SelectdbCloud.CommitTxnRequest;
import com.selectdb.cloud.proto.SelectdbCloud.CommitTxnResponse;
import com.selectdb.cloud.proto.SelectdbCloud.PrecommitTxnRequest;
import com.selectdb.cloud.proto.SelectdbCloud.PrecommitTxnResponse;
import com.selectdb.cloud.proto.SelectdbCloud.TxnInfoPB;
import com.selectdb.cloud.proto.SelectdbCloud.UniqueIdPB;
import com.selectdb.cloud.rpc.MetaServiceProxy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public class CloudGlobalTransactionMgr implements GlobalTransactionMgrInterface {
    private static final Logger LOG = LogManager.getLogger(CloudGlobalTransactionMgr.class);

    private Env env;

    private TxnStateCallbackFactory callbackFactory = new TxnStateCallbackFactory();

    public CloudGlobalTransactionMgr(Env env) {
        this.env = env;
    }

    public TxnStateCallbackFactory getCallbackFactory() {
        return callbackFactory;
    }

    @Override
    public DatabaseTransactionMgr getDatabaseTransactionMgr(long dbId) throws AnalysisException {
        //to do
        return null;
    }

    @Override
    public void addDatabaseTransactionMgr(Long dbId) {
        //do nothing
    }

    @Override
    public void removeDatabaseTransactionMgr(Long dbId) {
        //do nothing
    }

    @Override
    public long beginTransaction(long dbId, List<Long> tableIdList, String label, TxnCoordinator coordinator,
            LoadJobSourceType sourceType, long timeoutSecond)
            throws AnalysisException, LabelAlreadyUsedException, BeginTransactionException, DuplicatedRequestException,
            QuotaExceedException, MetaNotFoundException {
        return beginTransaction(dbId, tableIdList, label, null, coordinator, sourceType, -1, timeoutSecond);
    }

    @Override
    public long beginTransaction(long dbId, List<Long> tableIdList, String label, TUniqueId requestId,
            TxnCoordinator coordinator, LoadJobSourceType sourceType, long listenerId, long timeoutSecond)
            throws AnalysisException, LabelAlreadyUsedException, BeginTransactionException, DuplicatedRequestException,
            QuotaExceedException, MetaNotFoundException {

        LOG.info("try to begin transaction.");
        if (Config.disable_load_job) {
            throw new AnalysisException("disable_load_job is set to true, all load jobs are prevented");
        }

        switch (sourceType) {
            case BACKEND_STREAMING:
                checkValidTimeoutSecond(timeoutSecond, Config.max_stream_load_timeout_second,
                        Config.min_load_timeout_second);
                break;
            default:
                checkValidTimeoutSecond(timeoutSecond, Config.max_load_timeout_second, Config.min_load_timeout_second);
        }

        Preconditions.checkNotNull(coordinator);
        Preconditions.checkNotNull(label);
        FeNameFormat.checkLabel(label);

        String metaServiceEndpoint = Config.meta_service_endpoint;
        String[] splitMetaServiceEndpoint = metaServiceEndpoint.split(":");

        TNetworkAddress metaAddress =
                new TNetworkAddress(splitMetaServiceEndpoint[0], Integer.parseInt(splitMetaServiceEndpoint[1]));

        TxnInfoPB.Builder txnInfoBuilder = TxnInfoPB.newBuilder();
        txnInfoBuilder.setDbId(dbId);
        txnInfoBuilder.addAllTableIds(tableIdList);
        txnInfoBuilder.setLabel(label);

        if (requestId != null) {
            UniqueIdPB.Builder uniqueIdBuilder = UniqueIdPB.newBuilder();
            uniqueIdBuilder.setHi(requestId.getHi());
            uniqueIdBuilder.setLo(requestId.getLo());
            txnInfoBuilder.setRequestUniqueId(uniqueIdBuilder);
        }

        txnInfoBuilder.setCoordinator(coordinator.toPB());
        txnInfoBuilder.setLoadJobSourceType(sourceType.toPB());
        txnInfoBuilder.setTimeoutSecond(timeoutSecond);

        final BeginTxnRequest beginTxnRequest = BeginTxnRequest.newBuilder()
                .setTxnInfo(txnInfoBuilder.build())
                .setCloudUniqueId(Config.cloud_unique_id)
                .build();
        try {
            LOG.info("beginTxnRequest: {}", beginTxnRequest);
            BeginTxnResponse beginTxnResponse = MetaServiceProxy.getInstance().beginTxn(metaAddress, beginTxnRequest);
            LOG.info("beginTxnResponse: {}", beginTxnResponse);
            return beginTxnResponse.getTxnId();
        } catch (RpcException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkValidTimeoutSecond(long timeoutSecond, int maxLoadTimeoutSecond,
            int minLoadTimeOutSecond) throws AnalysisException {
        if (timeoutSecond > maxLoadTimeoutSecond || timeoutSecond < minLoadTimeOutSecond) {
            throw new AnalysisException("Invalid timeout: " + timeoutSecond + ". Timeout should between "
                    + minLoadTimeOutSecond + " and " + maxLoadTimeoutSecond
                    + " seconds");
        }
    }

    @Override
    public TransactionStatus getLabelState(long dbId, String label) {
        return null;
    }

    @Override
    public Long getTransactionId(long dbId, String label) {
        return null;
    }

    @Override
    public void preCommitTransaction2PC(Database db, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis, TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        if (Config.disable_load_job) {
            throw new TransactionCommitFailedException("disable_load_job is set to true, all load jobs are prevented");
        }
        LOG.info("try to commit transaction: {}", transactionId);

        String metaServiceEndpoint = Config.meta_service_endpoint;
        String[] splitMetaServiceEndpoint = metaServiceEndpoint.split(":");

        TNetworkAddress metaAddress =
                new TNetworkAddress(splitMetaServiceEndpoint[0], Integer.parseInt(splitMetaServiceEndpoint[1]));

        PrecommitTxnRequest.Builder builder = PrecommitTxnRequest.newBuilder();
        builder.setDbId(db.getId());
        builder.setTxnId(transactionId);

        final PrecommitTxnRequest precommitTxnRequest = builder.build();
        try {
            LOG.info("precommitTxnRequest: {}", precommitTxnRequest);
            PrecommitTxnResponse precommitTxnResponse = MetaServiceProxy
                    .getInstance().precommitTxn(metaAddress, precommitTxnRequest);
            LOG.info("precommitTxnResponse: {}", precommitTxnResponse);
            return;
        } catch (RpcException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void preCommitTransaction2PC(long dbId, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, TxnCommitAttachment txnCommitAttachment)
            throws UserException {

    }

    @Override
    public void commitTransaction(long dbId, List<Table> tableList,
            long transactionId, List<TabletCommitInfo> tabletCommitInfos)
            throws UserException {
        commitTransaction(dbId, tableList, transactionId, tabletCommitInfos, null);
    }

    @Override
    public void commitTransaction(long dbId, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        if (Config.disable_load_job) {
            throw new TransactionCommitFailedException("disable_load_job is set to true, all load jobs are prevented");
        }
        LOG.info("try to commit transaction: {}", transactionId);

        String metaServiceEndpoint = Config.meta_service_endpoint;
        String[] splitMetaServiceEndpoint = metaServiceEndpoint.split(":");

        TNetworkAddress metaAddress =
                new TNetworkAddress(splitMetaServiceEndpoint[0], Integer.parseInt(splitMetaServiceEndpoint[1]));

        CommitTxnRequest.Builder builder = CommitTxnRequest.newBuilder();
        builder.setDbId(dbId);
        builder.setTxnId(transactionId);
        builder.setIs2Pc(false);

        final CommitTxnRequest commitTxnRequest = builder.build();
        try {
            LOG.info("commitTxnRequest:{}", commitTxnRequest);
            CommitTxnResponse commitTxnResponse = MetaServiceProxy
                    .getInstance().commitTxn(metaAddress, commitTxnRequest);
            LOG.info("commitTxnResponse: {}", commitTxnResponse);
            return;
        } catch (RpcException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean commitAndPublishTransaction(Database db, List<Table> tableList, long transactionId,
                                               List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis)
            throws UserException {
        return commitAndPublishTransaction(db, tableList, transactionId, tabletCommitInfos, timeoutMillis, null);
    }

    @Override
    public boolean commitAndPublishTransaction(Database db, List<Table> tableList, long transactionId,
                                               List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis,
                                               TxnCommitAttachment txnCommitAttachment) throws UserException {

        commitTransaction(db.getId(), tableList, transactionId, tabletCommitInfos, txnCommitAttachment);
        return true;
    }

    @Override
    public void commitTransaction2PC(Database db, List<Table> tableList, long transactionId, long timeoutMillis)
            throws UserException {
    }

    @Override
    public void abortTransaction(long dbId, long transactionId, String reason) throws UserException {
    }

    @Override
    public void abortTransaction(Long dbId, Long txnId, String reason,
            TxnCommitAttachment txnCommitAttachment) throws UserException {
    }

    @Override
    public void abortTransaction(Long dbId, String label, String reason) throws UserException {

    }

    @Override
    public void abortTransaction2PC(Long dbId, long transactionId) throws UserException {

    }

    @Override
    public List<TransactionState> getReadyToPublishTransactions() {
        return null;
    }

    @Override
    public boolean existCommittedTxns(Long dbId, Long tableId, Long partitionId) {
        return false;
    }

    @Override
    public void finishTransaction(long dbId, long transactionId, Set<Long> errorReplicaIds) throws UserException {

    }

    @Override
    public boolean isPreviousTransactionsFinished(long endTransactionId, long dbId, List<Long> tableIdList)
            throws AnalysisException {
        return false;
    }

    @Override
    public void removeExpiredAndTimeoutTxns() {

    }

    @Override
    public TransactionState getTransactionState(long dbId, long transactionId) {
        return null;
    }

    public void setEditLog(EditLog editLog) {
    }

    public void replayUpsertTransactionState(TransactionState transactionState) throws MetaNotFoundException {

    }

    @Deprecated
    public void replayDeleteTransactionState(TransactionState transactionState) throws MetaNotFoundException {

    }

    @Override
    public void replayBatchRemoveTransactions(BatchRemoveTransactionsOperation operation) {

    }

    public List<List<Comparable>> getDbInfo() {
        return null;
    }

    public List<List<String>> getDbTransStateInfo(long dbId) {
        return null;
    }

    public List<List<String>> getDbTransInfo(long dbId, boolean running, int limit) throws AnalysisException {
        return null;
    }

    public List<List<String>> getDbTransInfoByStatus(long dbId, TransactionStatus status) throws AnalysisException {
        return null;
    }

    public List<List<String>> getSingleTranInfo(long dbId, long txnId) throws AnalysisException {
        return null;
    }

    public List<List<Comparable>> getTableTransInfo(long dbId, long txnId) throws AnalysisException {
        return null;
    }

    public List<List<Comparable>> getPartitionTransInfo(long dbId, long tid, long tableId)
            throws AnalysisException {
        return null;
    }

    @Override
    public int getTransactionNum() {
        int txnNum = 0;
        return txnNum;
    }

    @Override
    public long getNextTransactionId(long dbId) {
        return 0;
    }

    @Override
    public void abortTxnWhenCoordinateBeDown(String coordinateHost, int limit) {
        //do nothing
    }

    @Override
    public void updateDatabaseUsedQuotaData(long dbId, long usedQuotaDataBytes) throws AnalysisException {
        //do nothing
    }

    @Override
    public TWaitingTxnStatusResult getWaitingTxnStatus(TWaitingTxnStatusRequest request)
            throws AnalysisException, TimeoutException {
        return null;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        //do nothing
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        //do nothing
    }

    @Override
    public long getTxnNumByStatus(TransactionStatus status) {
        // TODO Auto-generated method stub
        return 0;
    }
}
