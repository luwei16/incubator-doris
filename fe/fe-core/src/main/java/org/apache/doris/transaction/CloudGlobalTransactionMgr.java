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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.QuotaExceedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.load.loadv2.LoadJobFinalOperation;
import org.apache.doris.persist.BatchRemoveTransactionsOperation;
import org.apache.doris.persist.EditLog;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TWaitingTxnStatusRequest;
import org.apache.doris.thrift.TWaitingTxnStatusResult;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.selectdb.cloud.proto.SelectdbCloud.AbortTxnRequest;
import com.selectdb.cloud.proto.SelectdbCloud.AbortTxnResponse;
import com.selectdb.cloud.proto.SelectdbCloud.BeginTxnRequest;
import com.selectdb.cloud.proto.SelectdbCloud.BeginTxnResponse;
import com.selectdb.cloud.proto.SelectdbCloud.CommitTxnRequest;
import com.selectdb.cloud.proto.SelectdbCloud.CommitTxnResponse;
import com.selectdb.cloud.proto.SelectdbCloud.MetaServiceCode;
import com.selectdb.cloud.proto.SelectdbCloud.PrecommitTxnRequest;
import com.selectdb.cloud.proto.SelectdbCloud.PrecommitTxnResponse;
import com.selectdb.cloud.proto.SelectdbCloud.TxnInfoPB;
import com.selectdb.cloud.proto.SelectdbCloud.UniqueIdPB;
import com.selectdb.cloud.rpc.MetaServiceProxy;
import com.selectdb.cloud.transaction.TxnUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CloudGlobalTransactionMgr implements GlobalTransactionMgrInterface {
    private static final Logger LOG = LogManager.getLogger(CloudGlobalTransactionMgr.class);

    private Env env;

    private TxnStateCallbackFactory callbackFactory = new TxnStateCallbackFactory();

    // TODO(zhanglei): reclaim committed transactions if txn timed-out (expired)
    // {DbId, TxnId} -> TxnState
    private com.google.common.collect.Table<Long, Long, TransactionState> dbIdTxnIdToTxnState = HashBasedTable.create();
    private final ReentrantReadWriteLock txnLock = new ReentrantReadWriteLock(true);

    public CloudGlobalTransactionMgr(Env env) {
        this.env = env;
    }

    public TxnStateCallbackFactory getCallbackFactory() {
        return callbackFactory;
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

        TNetworkAddress metaAddress = getMetaSerivceAddress();

        TxnInfoPB.Builder txnInfoBuilder = TxnInfoPB.newBuilder();
        txnInfoBuilder.setDbId(dbId);
        txnInfoBuilder.addAllTableIds(tableIdList);
        txnInfoBuilder.setLabel(label);

        if (requestId != null) {
            UniqueIdPB.Builder uniqueIdBuilder = UniqueIdPB.newBuilder();
            uniqueIdBuilder.setHi(requestId.getHi());
            uniqueIdBuilder.setLo(requestId.getLo());
            txnInfoBuilder.setRequestId(uniqueIdBuilder);
        }

        txnInfoBuilder.setCoordinator(coordinator.toPB());
        txnInfoBuilder.setLoadJobSourceType(sourceType.toPB());
        txnInfoBuilder.setTimeoutMs(timeoutSecond * 1000);

        final BeginTxnRequest beginTxnRequest = BeginTxnRequest.newBuilder()
                .setTxnInfo(txnInfoBuilder.build())
                .setCloudUniqueId(Config.cloud_unique_id)
                .build();
        BeginTxnResponse beginTxnResponse = null;
        try {
            LOG.info("beginTxnRequest: {}", beginTxnRequest);
            beginTxnResponse = MetaServiceProxy.getInstance().beginTxn(metaAddress, beginTxnRequest);
            LOG.info("beginTxnResponse: {}", beginTxnResponse);
        } catch (RpcException e) {
            LOG.warn("beginTransaction() rpc failed, RpcException= {}", e);
            throw new BeginTransactionException("beginTransaction() rpc failed, RpcException=" + e.toString());
        }

        Preconditions.checkNotNull(beginTxnResponse);
        Preconditions.checkNotNull(beginTxnResponse.getStatus());
        switch (beginTxnResponse.getStatus().getCode()) {
            case OK:
                break;
            case TXN_DUPLICATED_REQ:
                throw new DuplicatedRequestException(DebugUtil.printId(requestId),
                        beginTxnResponse.getDupTxnId(), beginTxnResponse.getStatus().getMsg());
            case TXN_LABEL_ALREADY_USED:
                throw new LabelAlreadyUsedException(beginTxnResponse.getStatus().getMsg());
            default:
                throw new BeginTransactionException(beginTxnResponse.getStatus().getMsg());
        }

        long txnId = beginTxnResponse.getTxnId();

        txnLock.writeLock().lock();
        try {
            TransactionState state = new TransactionState(dbId, tableIdList, txnId, label, requestId, sourceType,
                                                          coordinator, listenerId, timeoutSecond * 1000);
            state.setTransactionStatus(TransactionStatus.PREPARE);
            // Ignore existing
            dbIdTxnIdToTxnState.put(dbId, beginTxnResponse.getTxnId(), state);
        } finally {
            txnLock.writeLock().unlock();
        }

        return txnId;
    }

    @Override
    public void preCommitTransaction2PC(Database db, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis, TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        LOG.info("try to precommit transaction: {}", transactionId);
        if (Config.disable_load_job) {
            throw new TransactionCommitFailedException("disable_load_job is set to true, all load jobs are prevented");
        }

        TNetworkAddress metaAddress = getMetaSerivceAddress();

        PrecommitTxnRequest.Builder builder = PrecommitTxnRequest.newBuilder();
        builder.setDbId(db.getId());
        builder.setTxnId(transactionId);

        if (txnCommitAttachment != null) {
            if (txnCommitAttachment instanceof LoadJobFinalOperation) {
                LoadJobFinalOperation loadJobFinalOperation = (LoadJobFinalOperation) txnCommitAttachment;
                builder.setTxnCommitAttachment(TxnUtil
                        .loadJobFinalOperationToPb(loadJobFinalOperation));
            } else {
                throw new UserException("Invalid txnCommitAttachment");
            }
        }

        final PrecommitTxnRequest precommitTxnRequest = builder.build();
        PrecommitTxnResponse precommitTxnResponse = null;
        try {
            LOG.info("precommitTxnRequest: {}", precommitTxnRequest);
            precommitTxnResponse = MetaServiceProxy
                    .getInstance().precommitTxn(metaAddress, precommitTxnRequest);
            LOG.info("precommitTxnResponse: {}", precommitTxnResponse);
        } catch (RpcException e) {
            throw new TransactionCommitFailedException(e.getMessage());
        }

        if (precommitTxnResponse.getStatus().getCode() != MetaServiceCode.OK) {
            throw new TransactionCommitFailedException(precommitTxnResponse.getStatus().getMsg());
        }
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
        commitTransaction(dbId, tableList, transactionId, tabletCommitInfos, txnCommitAttachment, false);
    }

    private void commitTransaction(long dbId, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, TxnCommitAttachment txnCommitAttachment, boolean is2PC)
            throws UserException {

        LOG.info("try to commit transaction: {}", transactionId);
        if (Config.disable_load_job) {
            throw new TransactionCommitFailedException("disable_load_job is set to true, all load jobs are prevented");
        }

        TNetworkAddress metaAddress = getMetaSerivceAddress();
        CommitTxnRequest.Builder builder = CommitTxnRequest.newBuilder();
        builder.setDbId(dbId)
                .setTxnId(transactionId)
                .setIs2Pc(is2PC)
                .setCloudUniqueId(Config.cloud_unique_id);

        if (txnCommitAttachment != null) {
            if (txnCommitAttachment instanceof LoadJobFinalOperation) {
                LoadJobFinalOperation loadJobFinalOperation = (LoadJobFinalOperation) txnCommitAttachment;
                builder.setTxnCommitAttachment(TxnUtil
                        .loadJobFinalOperationToPb(loadJobFinalOperation));
            } else {
                throw new UserException("Invalid txnCommitAttachment");
            }
        }

        txnLock.writeLock().lock();
        TransactionState state = dbIdTxnIdToTxnState.get(dbId, transactionId);
        try {
            if (state == null) {
                state = new TransactionState();
                dbIdTxnIdToTxnState.put(dbId, transactionId, state);
                LOG.warn("impossible branch reached, dbId={} txnId={} is2PC", dbId, transactionId, is2PC);
            }
        } finally {
            txnLock.writeLock().unlock();
        }

        final CommitTxnRequest commitTxnRequest = builder.build();
        try {
            LOG.info("commitTxnRequest:{}", commitTxnRequest);
            CommitTxnResponse commitTxnResponse = MetaServiceProxy
                    .getInstance().commitTxn(metaAddress, commitTxnRequest);
            LOG.info("commitTxnResponse: {}", commitTxnResponse);
            if (commitTxnResponse.getStatus().getCode() != MetaServiceCode.OK) {
                throw new Exception(commitTxnResponse.getStatus().getMsg());
            }
            TxnStateChangeCallback cb = callbackFactory.getCallback(state.getCallbackId());
            if (cb == null) {
                LOG.info("no callback to run for this txn, txnId={} callbackId={}", transactionId,
                         state.getCallbackId());
                return;
            }
            LOG.info("run txn callback, txnId={} callbackId={}", transactionId, state.getCallbackId());
            state.setTransactionStatus(TransactionStatus.COMMITTED);
            cb.afterCommitted(state, true);
            state.setTransactionStatus(TransactionStatus.VISIBLE);
            cb.afterVisible(state, true);
        } catch (Exception e) {
            state.setTransactionStatus(TransactionStatus.ABORTED);
            LOG.info("failed to commit txn {}", e.getMessage(), e);
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
        commitTransaction(db.getId(), tableList, transactionId, null, null, true);
    }

    @Override
    public void abortTransaction(long dbId, long transactionId, String reason) throws UserException {
        abortTransaction(dbId, transactionId, reason, null);
    }

    @Override
    public void abortTransaction(Long dbId, Long transactionId, String reason,
            TxnCommitAttachment txnCommitAttachment) throws UserException {
        LOG.info("try to abort transaction, dbId:{}, transactionId:{}", dbId, transactionId);

        TNetworkAddress metaAddress = getMetaSerivceAddress();
        AbortTxnRequest.Builder builder = AbortTxnRequest.newBuilder();
        builder.setDbId(dbId);
        builder.setTxnId(transactionId);
        builder.setReason(reason);

        final AbortTxnRequest abortTxnRequest = builder.build();
        AbortTxnResponse abortTxnResponse = null;
        try {
            LOG.info("abortTxnRequest:{}", abortTxnRequest);
            abortTxnResponse = MetaServiceProxy
                    .getInstance().abortTxn(metaAddress, abortTxnRequest);
            LOG.info("abortTxnResponse: {}", abortTxnResponse);
        } catch (RpcException e) {
            throw new UserException(e);
        }

        if (abortTxnResponse.getStatus().getCode() != MetaServiceCode.OK) {
            throw new UserException(abortTxnResponse.getStatus().getMsg());
        }
    }

    @Override
    public void abortTransaction(Long dbId, String label, String reason) throws UserException {
        LOG.info("try to abort transaction, label:{}, transactionId:{}", dbId, label);

        TNetworkAddress metaAddress = getMetaSerivceAddress();
        AbortTxnRequest.Builder builder = AbortTxnRequest.newBuilder();
        builder.setDbId(dbId);
        builder.setLabel(label);
        builder.setReason(reason);

        final AbortTxnRequest abortTxnRequest = builder.build();
        AbortTxnResponse abortTxnResponse = null;
        try {
            LOG.info("abortTxnRequest:{}", abortTxnRequest);
            abortTxnResponse = MetaServiceProxy
                    .getInstance().abortTxn(metaAddress, abortTxnRequest);
            LOG.info("abortTxnResponse: {}", abortTxnResponse);
        } catch (RpcException e) {
            throw new UserException(e);
        }

        if (abortTxnResponse.getStatus().getCode() != MetaServiceCode.OK) {
            throw new UserException(abortTxnResponse.getStatus().getMsg());
        }
    }

    @Override
    public void abortTransaction2PC(Long dbId, long transactionId) throws UserException {
        LOG.info("try to abort 2pc transaction, dbId:{}, transactionId:{}", dbId, transactionId);

        TNetworkAddress metaAddress = getMetaSerivceAddress();
        AbortTxnRequest.Builder builder = AbortTxnRequest.newBuilder();
        builder.setDbId(dbId);
        builder.setTxnId(transactionId);

        final AbortTxnRequest abortTxnRequest = builder.build();
        AbortTxnResponse abortTxnResponse = null;
        try {
            LOG.info("abortTxnRequest:{}", abortTxnRequest);
            abortTxnResponse = MetaServiceProxy
                    .getInstance().abortTxn(metaAddress, abortTxnRequest);
            LOG.info("abortTxnResponse: {}", abortTxnResponse);
        } catch (RpcException e) {
            throw new UserException(e);
        }

        if (abortTxnResponse.getStatus().getCode() != MetaServiceCode.OK) {
            throw new UserException(abortTxnResponse.getStatus().getMsg());
        }
    }

    @Override
    public List<TransactionState> getReadyToPublishTransactions() {
        //do nothing for CloudGlobalTransactionMgr
        return new ArrayList<TransactionState>();
    }

    @Override
    public boolean existCommittedTxns(Long dbId, Long tableId, Long partitionId) {
        //do nothing for CloudGlobalTransactionMgr
        return false;
    }

    @Override
    public void finishTransaction(long dbId, long transactionId, Set<Long> errorReplicaIds) throws UserException {
        //do nothing for CloudGlobalTransactionMgr
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
        txnLock.readLock().lock();
        try {
            TransactionState state = dbIdTxnIdToTxnState.get(dbId, transactionId);
            if (state == null) {
                state = new TransactionState();
            }
            return state;
        } finally {
            txnLock.readLock().unlock();
        }
    }

    public void setEditLog(EditLog editLog) {
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
    public TransactionStatus getLabelState(long dbId, String label) {
        return null;
    }

    @Override
    public Long getTransactionId(long dbId, String label) {
        return null;
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
        long dbId = request.getDbId();
        int commitTimeoutSec = Config.commit_timeout_second;
        for (int i = 0; i < commitTimeoutSec; ++i) {
            Env.getCurrentInternalCatalog().getDbOrAnalysisException(dbId);
            TWaitingTxnStatusResult statusResult = new TWaitingTxnStatusResult();
            statusResult.status = new TStatus();
            TransactionStatus txnStatus = null;
            if (request.isSetTxnId()) {
                long txnId = request.getTxnId();
                TransactionState txnState = Env.getCurrentGlobalTransactionMgr()
                        .getTransactionState(dbId, txnId);
                if (txnState == null) {
                    throw new AnalysisException("txn does not exist: " + txnId);
                }
                txnStatus = txnState.getTransactionStatus();
                if (!txnState.getReason().trim().isEmpty()) {
                    statusResult.status.setErrorMsgsIsSet(true);
                    statusResult.status.addToErrorMsgs(txnState.getReason());
                }
            } else {
                txnStatus = getLabelState(dbId, request.getLabel());
            }
            if (txnStatus == TransactionStatus.UNKNOWN || txnStatus.isFinalStatus()) {
                statusResult.setTxnStatusId(txnStatus.value());
                return statusResult;
            }
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                LOG.info("commit sleep exception.", e);
            }
        }
        throw new TimeoutException("Operation is timeout");
    }

    @Override
    public int getRunningTxnNums(long dbId) {
        return 0;
    }

    @Override
    public List<TransactionState> getPreCommittedTxnList(long dbId) {
        //todo
        return new ArrayList<TransactionState>();
    }

    @Override
    public void addTableIndexes(long dbId, long transactionId, OlapTable table) throws UserException{
        //do nothing
    }

    private TNetworkAddress getMetaSerivceAddress() {
        String metaServiceEndpoint = Config.meta_service_endpoint;
        String[] splitMetaServiceEndpoint = metaServiceEndpoint.split(":");

        TNetworkAddress metaAddress =
                new TNetworkAddress(splitMetaServiceEndpoint[0], Integer.parseInt(splitMetaServiceEndpoint[1]));
        return metaAddress;
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
    public void write(DataOutput out) throws IOException {
        throw new IOException("Disallow to call wirte()");
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        throw new IOException("Disallow to call readFields()");
    }

    public void replayUpsertTransactionState(TransactionState transactionState) throws MetaNotFoundException {
        throw new MetaNotFoundException("Disallow to call replayUpsertTransactionState()");
    }

    @Deprecated
    // Use replayBatchDeleteTransactions instead
    public void replayDeleteTransactionState(TransactionState transactionState) throws MetaNotFoundException {
        throw new MetaNotFoundException("Disallow to call replayDeleteTransactionState()");
    }

    public void replayBatchRemoveTransactions(BatchRemoveTransactionsOperation operation) throws MetaNotFoundException {
        throw new MetaNotFoundException("Disallow to call replayBatchRemoveTransactions()");
    }

    @Override
    public long getTxnNumByStatus(TransactionStatus status) {
        // TODO Auto-generated method stub
        return 0;
    }
}
