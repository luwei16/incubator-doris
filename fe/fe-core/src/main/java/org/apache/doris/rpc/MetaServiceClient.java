package org.apache.doris.rpc;

import org.apache.doris.common.Config;
import org.apache.doris.proto.MetaService;
import org.apache.doris.proto.MetaService.PGetVisibleVersionResponse;
import org.apache.doris.proto.PMetaServiceGrpc;
import org.apache.doris.thrift.TNetworkAddress;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class MetaServiceClient {
    public static final Logger LOG = LogManager.getLogger(BackendServiceClient.class);

    private static final int MAX_RETRY_NUM = 0;
    private final TNetworkAddress address;
    private final PMetaServiceGrpc.PMetaServiceFutureStub stub;
    private final PMetaServiceGrpc.PMetaServiceBlockingStub blockingStub;
    private final ManagedChannel channel;

    public MetaServiceClient(TNetworkAddress address) {
        this.address = address;
        channel = NettyChannelBuilder.forAddress(address.getHostname(), address.getPort())
            .flowControlWindow(Config.grpc_max_message_size_bytes)
            .maxInboundMessageSize(Config.grpc_max_message_size_bytes).enableRetry().maxRetryAttempts(MAX_RETRY_NUM)
            .usePlaintext().build();
        stub = PMetaServiceGrpc.newFutureStub(channel);
        blockingStub = PMetaServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() {
        if (!channel.isShutdown()) {
            channel.shutdown();
            try {
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOG.warn("Timed out gracefully shutting down connection: {}. ", channel);
                }
            } catch (InterruptedException e) {
                return;
            }
        }

        if (!channel.isTerminated()) {
            channel.shutdownNow();
            try {
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOG.warn("Timed out forcefully shutting down connection: {}. ", channel);
                }
            } catch (InterruptedException e) {
                return;
            }
        }

        LOG.warn("shut down backend service client: {}", address);
    }

    public Future<PGetVisibleVersionResponse> getVisibleVersionAsync(MetaService.PGetVisibleVersionRequest request) {
        return stub.getVisibleVersion(request);
    }
}
