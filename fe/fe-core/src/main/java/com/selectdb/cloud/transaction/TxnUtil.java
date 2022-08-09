package com.selectdb.cloud.transaction;

import com.selectdb.cloud.proto.SelectdbCloud.TxnCommitAttachmentPB;
import com.selectdb.cloud.proto.SelectdbCloud.TxnCommitAttachmentPB.LoadJobFinalOperationPB;
import com.selectdb.cloud.proto.SelectdbCloud.TxnCommitAttachmentPB.LoadJobFinalOperationPB.EtlStatusPB;
import com.selectdb.cloud.proto.SelectdbCloud.TxnCommitAttachmentPB.LoadJobFinalOperationPB.FailMsgPB;
import com.selectdb.cloud.proto.SelectdbCloud.TxnCommitAttachmentPB.LoadJobFinalOperationPB.JobStatePB;

import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.loadv2.JobState;
import org.apache.doris.load.loadv2.LoadJobFinalOperation;
import org.apache.doris.thrift.TEtlState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TxnUtil {
    private static final Logger LOG = LogManager.getLogger(TxnUtil.class);

    public static EtlStatusPB.EtlStatePB etlStateToPb(TEtlState tEtlState) {
        switch (tEtlState) {
            case RUNNING:
                return EtlStatusPB.EtlStatePB.RUNNING;
            case FINISHED:
                return EtlStatusPB.EtlStatePB.FINISHED;
            case CANCELLED:
                return EtlStatusPB.EtlStatePB.CANCELLED;
            default:
                return EtlStatusPB.EtlStatePB.UNKNOWN;
        }
    }

    public static TEtlState etlStateFromPb(EtlStatusPB.EtlStatePB etlStatePB) {
        switch (etlStatePB) {
            case RUNNING:
                return TEtlState.RUNNING;
            case FINISHED:
                return TEtlState.FINISHED;
            case CANCELLED:
                return TEtlState.CANCELLED;
            default:
                return TEtlState.UNKNOWN;
        }
    }

    public static JobStatePB jobStateToPb(JobState jobState) {
        switch (jobState) {
            case PENDING:
                return JobStatePB.PENDING;
            case ETL:
                return JobStatePB.ETL;
            case LOADING:
                return JobStatePB.LOADING;
            case COMMITTED:
                return JobStatePB.COMMITTED;
            case FINISHED:
                return JobStatePB.FINISHED;
            case CANCELLED:
                return JobStatePB.CANCELLED;
            default:
                return JobStatePB.UNKNOWN;
        }
    }

    public static JobState jobStateFromPb(JobStatePB jobStatePb) {
        switch (jobStatePb) {
            case PENDING:
                return JobState.PENDING;
            case ETL:
                return JobState.ETL;
            case LOADING:
                return JobState.LOADING;
            case COMMITTED:
                return JobState.COMMITTED;
            case FINISHED:
                return JobState.FINISHED;
            case CANCELLED:
                return JobState.CANCELLED;
            default:
                return JobState.UNKNOWN;
        }
    }

    public static FailMsgPB failMsgToPb(FailMsg failMsg) {
        FailMsgPB.Builder builder = FailMsgPB.newBuilder();
        builder.setMsg(failMsg.getMsg());

        switch (failMsg.getCancelType()) {
            case USER_CANCEL:
                builder.setCancelType(FailMsgPB.CancelTypePB.USER_CANCEL);
                break;
            case ETL_SUBMIT_FAIL:
                builder.setCancelType(FailMsgPB.CancelTypePB.ETL_SUBMIT_FAIL);
                break;
            case ETL_RUN_FAIL:
                builder.setCancelType(FailMsgPB.CancelTypePB.ETL_RUN_FAIL);
                break;
            case ETL_QUALITY_UNSATISFIED:
                builder.setCancelType(FailMsgPB.CancelTypePB.ETL_QUALITY_UNSATISFIED);
                break;
            case LOAD_RUN_FAIL:
                builder.setCancelType(FailMsgPB.CancelTypePB.LOAD_RUN_FAIL);
                break;
            case TIMEOUT:
                builder.setCancelType(FailMsgPB.CancelTypePB.TIMEOUT);
                break;
            case TXN_UNKNOWN:
                builder.setCancelType(FailMsgPB.CancelTypePB.TXN_UNKNOWN);
                break;
            default:
                builder.setCancelType(FailMsgPB.CancelTypePB.UNKNOWN);
                break;
        }
        return builder.build();
    }

    public static FailMsg failMsgFromPb(FailMsgPB failMsgPb) {
        FailMsg failMsg = new FailMsg();
        failMsg.setMsg(failMsgPb.getMsg());
        switch (failMsgPb.getCancelType()) {
            case USER_CANCEL:
                failMsg.setCancelType(FailMsg.CancelType.USER_CANCEL);
                break;
            case ETL_SUBMIT_FAIL:
                failMsg.setCancelType(FailMsg.CancelType.ETL_SUBMIT_FAIL);
                break;
            case ETL_RUN_FAIL:
                failMsg.setCancelType(FailMsg.CancelType.ETL_RUN_FAIL);
                break;
            case ETL_QUALITY_UNSATISFIED:
                failMsg.setCancelType(FailMsg.CancelType.ETL_QUALITY_UNSATISFIED);
                break;
            case LOAD_RUN_FAIL:
                failMsg.setCancelType(FailMsg.CancelType.LOAD_RUN_FAIL);
                break;
            case TIMEOUT:
                failMsg.setCancelType(FailMsg.CancelType.TIMEOUT);
                break;
            case TXN_UNKNOWN:
                failMsg.setCancelType(FailMsg.CancelType.TXN_UNKNOWN);
                break;
            default:
                failMsg.setCancelType(FailMsg.CancelType.UNKNOWN);
                break;
        }
        return failMsg;
    }

    public static EtlStatusPB etlStatusToPb(EtlStatus etlStatus) {
        EtlStatusPB.Builder builder = EtlStatusPB.newBuilder();
        builder.setState(TxnUtil.etlStateToPb(etlStatus.getState()))
                .setTrackingUrl(etlStatus.getTrackingUrl())
                .putAllStats(etlStatus.getStats())
                .putAllCounters(etlStatus.getCounters());
        return builder.build();
    }

    public static EtlStatus etlStatusFromPb(EtlStatusPB etlStatusPB) {
        EtlStatus etlStatus = new EtlStatus();

        etlStatus.setState(TxnUtil.etlStateFromPb(etlStatusPB.getState()));
        etlStatus.setTrackingUrl(etlStatusPB.getTrackingUrl());
        etlStatus.setStats(etlStatusPB.getStats());
        etlStatus.setCounters(etlStatusPB.getCounters());
        return etlStatus;
    }

    public static TxnCommitAttachmentPB loadJobFinalOperationToPb(LoadJobFinalOperation loadJobFinalOperation)  {
        TxnCommitAttachmentPB.Builder builder = TxnCommitAttachmentPB.newBuilder();
        builder.setType(TxnCommitAttachmentPB.Type.LODD_JOB_FINAL_OPERATION);

        builder.setLoadJobFinalOperation(TxnCommitAttachmentPB.LoadJobFinalOperationPB.newBuilder()
                .setId(loadJobFinalOperation.getId())
                .setLoadingStatus(TxnUtil.etlStatusToPb(loadJobFinalOperation.getLoadingStatus()))
                .setProgress(loadJobFinalOperation.getProgress())
                .setLoadStartTimestamp(loadJobFinalOperation.getLoadStartTimestamp())
                .setFinishTimestamp(loadJobFinalOperation.getFinishTimestamp())
                .setJobState(TxnUtil.jobStateToPb(loadJobFinalOperation.getJobState()))
                .setFailMsg(TxnUtil.failMsgToPb(loadJobFinalOperation.getFailMsg())));
        return builder.build();
    }

    public static LoadJobFinalOperation loadJobFinalOperationFromPb(TxnCommitAttachmentPB txnCommitAttachmentPB)  {
        LoadJobFinalOperationPB loadJobFinalOperationPB = txnCommitAttachmentPB.getLoadJobFinalOperation();
        return new LoadJobFinalOperation(loadJobFinalOperationPB.getId(),
                TxnUtil.etlStatusFromPb(loadJobFinalOperationPB.getLoadingStatus()),
                loadJobFinalOperationPB.getProgress(), loadJobFinalOperationPB.getLoadStartTimestamp(),
                loadJobFinalOperationPB.getFinishTimestamp(),
                TxnUtil.jobStateFromPb(loadJobFinalOperationPB.getJobState()),
                TxnUtil.failMsgFromPb(loadJobFinalOperationPB.getFailMsg()));
    }
}
