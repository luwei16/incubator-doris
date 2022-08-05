package com.selectdb.cloud.catalog;

import com.selectdb.cloud.proto.SelectdbCloud;
import com.selectdb.cloud.rpc.MetaServiceProxy;

import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.Config;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Internal representation of partition-related metadata.
 */
// TODO(dx): cache version
public class CloudPartition extends Partition {
    private static final Logger LOG = LogManager.getLogger(CloudPartition.class);

    // not Serialized
    private long dbId;
    private long tableId;

    public CloudPartition(long id, String name, MaterializedIndex baseIndex,
                          DistributionInfo distributionInfo, long dbId, long tableId) {
        super(id, name, baseIndex, distributionInfo);
        super.nextVersion = -1;
        this.dbId = dbId;
        this.tableId = tableId;
    }

    public CloudPartition() {
        super();
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    protected void setVisibleVersion(long visibleVersion) {
        LOG.info("setVisibleVersion use CloudPartition {}", super.getName());
        return;
    }

    @Override
    public long getVisibleVersion() {
        //TODO(dx): this log just for debug, delete later.
        LOG.info("getVisibleVersion use CloudPartition {}", super.getName());
        long version = getVersionFromMeta(System.currentTimeMillis() + 3000);
        if (version != -1) {
            // get version from metaService success, set visible version
            super.setVisibleVersion(version);
        } else {
            // failed
            LOG.warn("failed to get version from metaService, so use old version");
            version = super.getVisibleVersion();
        }
        return version;
    }

    @Override
    public long getNextVersion() {
        // use meta service visibleVersion
        //TODO(dx): this log just for debug, delete later.
        LOG.info("getNextVersion use CloudPartition {}", super.getName());
        return -1;
    }

    @Override
    public void setNextVersion(long nextVersion) {
        // use meta service visibleVersion
        //TODO(dx): this log just for debug, delete later.
        LOG.info("setNextVersion use CloudPartition {} Version {}", super.getName(), nextVersion);
        return;
    }

    @Override
    public void updateVersionForRestore(long visibleVersion) {
        //TODO(dx): this log just for debug, delete later.
        LOG.info("updateVersionForRestore use CloudPartition {} version for restore: visible: {}",
                super.getName(), visibleVersion);
        return;
    }

    @Override
    public void updateVisibleVersion(long visibleVersion) {
        // use meta service visibleVersion
        //TODO(dx): this log just for debug, delete later.
        LOG.info("updateVisibleVersion use CloudPartition {} version for restore: visible: {}",
                super.getName(), visibleVersion);

        return;
    }

    @Override
    public void updateVisibleVersionAndTime(long visibleVersion, long visibleVersionTime) {
        //TODO(dx): this log just for debug, delete later.
    }

    /**
     * CloudPartition always has data
     */
    @Override
    public boolean hasData() {
        return true;
    }

    /**
     * calc all constant exprs by BE
     * @return failed: -1, success: other
     */
    public long getVersionFromMeta(long timeoutTs) {
        TNetworkAddress metaAddress =
                new TNetworkAddress(Env.getCurrentSystemInfo().metaServiceHostPort.first,
                    Env.getCurrentSystemInfo().metaServiceHostPort.second);
        SelectdbCloud.GetVersionRequest.Builder builder = SelectdbCloud.GetVersionRequest.newBuilder();
        builder.setCloudUniqueId(Config.cloud_unique_id)
               .setDbId(this.dbId)
               .setTableId(this.tableId)
               .setPartitionId(super.getId());
        final SelectdbCloud.GetVersionRequest pRequest = builder.build();

        try {
            Future<SelectdbCloud.GetVersionResponse> future =
                    MetaServiceProxy.getInstance().getVisibleVersionAsync(metaAddress, pRequest);

            SelectdbCloud.GetVersionResponse pResult = null;
            while (pResult == null) {
                long currentTs = System.currentTimeMillis();
                if (currentTs >= timeoutTs) {
                    throw new TimeoutException("query timeout");
                }
                try {
                    pResult = future.get(timeoutTs - currentTs, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    // continue to get result
                    LOG.warn("future get interrupted Exception", e);
                }
            }

            if (pResult.hasStatus() && pResult.getStatus().hasCode()
                    && pResult.getStatus().getCode() != SelectdbCloud.MetaServiceCode.OK) {
                // meta Service return -1 or -2
                // -1 txn error
                // -2 no such key
                LOG.warn("get version from meta error, code : {}, msg: {}",
                        pResult.getStatus().getCode(),
                        Optional.ofNullable(pResult.getStatus().getMsg()).orElse(""));
                return -1;
            } else if (pResult.hasStatus() && pResult.getStatus().hasCode()) {
                // java8 lambda syntax, Assignment function ref.
                AtomicLong version = new AtomicLong(-1);
                Optional.ofNullable(pResult.getVersion()).ifPresent(version::set);
                LOG.info("get version {}", version);
                return version.get();
            }
            LOG.warn("get version from meta, not set error status");
            return -1;
        } catch (RpcException e) {
            LOG.warn("get version from meta RpcException: ", e);
            return -1;
        } catch (ExecutionException e) {
            LOG.warn("get version from meta ExecutionException: ", e);
            return -1;
        } catch (TimeoutException e) {
            LOG.warn("get version from meta TimeoutException: ", e);
            return -1;
        }
    }

    public static CloudPartition read(DataInput in) throws IOException {
        CloudPartition partition = new CloudPartition();
        partition.readFields(in);
        partition.setDbId(in.readLong());
        partition.setTableId(in.readLong());
        return partition;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeLong(this.dbId);
        out.writeLong(this.tableId);
    }

    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }

        if (!(obj instanceof CloudPartition)) {
            return false;
        }
        CloudPartition cloudPartition = (CloudPartition) obj;
        return (dbId == cloudPartition.dbId) && (tableId == cloudPartition.tableId);
    }

    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(super.toString());
        buffer.append("dbId: ").append(this.dbId).append("; ");
        buffer.append("tableId: ").append(this.tableId).append("; ");
        return buffer.toString();
    }
}
