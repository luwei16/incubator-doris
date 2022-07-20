package com.selectdb.cloud.rpc;

import com.selectdb.cloud.proto.SelectdbCloud;

import com.google.common.collect.Maps;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

public class MetaServiceProxy {
    private static final Logger LOG = LogManager.getLogger(MetaServiceProxy.class);
    // use exclusive lock to make sure only one thread can add or remove client from serviceMap.
    // use concurrent map to allow access serviceMap in multi thread.
    private ReentrantLock lock = new ReentrantLock();

    private final Map<TNetworkAddress, MetaServiceClient> serviceMap;

    public MetaServiceProxy() {
        this.serviceMap = Maps.newConcurrentMap();
    }

    private static class SingletonHolder {
        private static final MetaServiceProxy INSTANCE = new MetaServiceProxy();
    }

    public static MetaServiceProxy getInstance() {
        return MetaServiceProxy.SingletonHolder.INSTANCE;
    }

    public void removeProxy(TNetworkAddress address) {
        LOG.warn("begin to remove proxy: {}", address);
        MetaServiceClient service;
        lock.lock();
        try {
            service = serviceMap.remove(address);
        } finally {
            lock.unlock();
        }

        if (service != null) {
            service.shutdown();
        }
    }

    private MetaServiceClient getProxy(TNetworkAddress address) {
        MetaServiceClient service = serviceMap.get(address);
        if (service != null) {
            return service;
        }

        // not exist, create one and return.
        lock.lock();
        try {
            service = serviceMap.get(address);
            if (service == null) {
                service = new MetaServiceClient(address);
                serviceMap.put(address, service);
            }
            return service;
        } finally {
            lock.unlock();
        }
    }

    public Future<SelectdbCloud.GetVersionResponse>
            getVisibleVersionAsync(TNetworkAddress address, SelectdbCloud.GetVersionRequest request)
            throws RpcException {
        try {
            final MetaServiceClient client = getProxy(address);
            return client.getVisibleVersionAsync(request);
        } catch (Exception e) {
            throw new RpcException(address.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.GetVersionResponse
            getVersion(TNetworkAddress address, SelectdbCloud.GetVersionRequest request) throws RpcException {
        try {
            final MetaServiceClient client = getProxy(address);
            return client.getVersion(request);
        } catch (Exception e) {
            throw new RpcException(address.hostname, e.getMessage(), e);
        }
    }
}
