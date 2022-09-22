/*
 * Copyright (c) 2016 Rackspace.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.rackspacecloud.blueflood.io.datastax;

import com.codahale.metrics.*;
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;

import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.IOConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Set;

/**
 * This class is a singleton that holds the necessary code that uses datastax
 * driver to make connections to Cassandra. All of the consumer code needing
 * to use datastax driver can use this class to get a {@link com.datastax.driver.core.Session}
 * object for the read/write statements to Cassandra.
 */
public class DatastaxIO implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(DatastaxIO.class);
    private static final String metricsPrefix = "datastax.";

    private final IOConfig ioconfig = IOConfig.singleton();
    private final Cluster cluster;
    private final Session session;

    public enum Keyspace { NO_KEYSPACE, DATA_KEYSPACE }
    private static DatastaxIO INSTANCE;

    public static DatastaxIO getInstance() {
        // Have to lazy init this, or it might try to connect to the DATA keyspace before a test server has come up and
        // been initialized.
        if (INSTANCE == null) {
             INSTANCE = new DatastaxIO(Keyspace.DATA_KEYSPACE, true);
        }
        return INSTANCE;
    }

    /**
     * Oh Java, how I wish you had tuples.
     */
    private static class ClusterAndSession {
        final Cluster cluster;
        final Session session;

        public ClusterAndSession(Cluster cluster, Session session) {
            this.cluster = cluster;
            this.session = session;
        }
    }

    public DatastaxIO(DatastaxIO.Keyspace keyspace, boolean monitor) {
        ClusterAndSession result;
        if (keyspace == Keyspace.NO_KEYSPACE) {
            result = connect(null);
        } else if (keyspace == Keyspace.DATA_KEYSPACE) {
            result = connect(CassandraModel.QUOTED_KEYSPACE);
        } else {
            throw new IllegalArgumentException("No such keyspace setting: " + keyspace);
        }
        cluster = result.cluster;
        session = result.session;
        if ( LOG.isDebugEnabled() ) {
            logDebugConnectionInfo();
        }
        if (monitor) {
            monitorConnection();
        }
    }

    private ClusterAndSession connect(String keyspace) {

        Set<InetSocketAddress> dbHosts = ioconfig.getUniqueBinaryTransportHostsAsInetSocketAddresses();

        int readTimeoutMaxRetries = ioconfig.getReadTimeoutMaxRetries();
        int writeTimeoutMaxRetries = ioconfig.getWriteTimeoutMaxRetries();
        int unavailableMaxRetries = ioconfig.getUnavailableMaxRetries();

        CodecRegistry codecRegistry = new CodecRegistry();

        Cluster cluster = Cluster.builder()
                .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().withLocalDc(ioconfig.getDatacenterName()).build(), false))
                .withPoolingOptions(getPoolingOptions())
                .withRetryPolicy(new RetryNTimes(readTimeoutMaxRetries, writeTimeoutMaxRetries, unavailableMaxRetries))
                .withCodecRegistry(codecRegistry)
                .withSocketOptions(getSocketOptions())
                .addContactPointsWithPorts(dbHosts)
                .build();

        QueryLogger queryLogger = QueryLogger.builder()
                .withConstantThreshold(5000)
                .build();

        cluster.register(queryLogger);

        Session session;
        try {
            if (keyspace == null) {
                session = cluster.connect();
            } else {
                session = cluster.connect(keyspace);
            }
        }
        catch (NoHostAvailableException e){
            // TODO: figure out how to bubble this up
            throw new RuntimeException(e);
        }
        return new ClusterAndSession(cluster, session);
    }

    private void logDebugConnectionInfo() {
        if ( cluster == null ) {
            throw new IllegalStateException("cluster is not initialized");
        }
        final Metadata metadata = cluster.getMetadata();
        LOG.debug("Connected to cluster: " + metadata.getClusterName());
        for (final Host host : metadata.getAllHosts()) {
            LOG.debug(String.format("Datacenter: %s; Host: %s; Rack: %s\n",
                      host.getDatacenter(), host.getAddress(), host.getRack()));
        }
    }

    private SocketOptions getSocketOptions() {
        final SocketOptions socketOptions = new SocketOptions();
        socketOptions.setConnectTimeoutMillis(ioconfig.getRequestTimeout())
                     .setReadTimeoutMillis(ioconfig.getRequestTimeout());
        return socketOptions;
    }

    private PoolingOptions getPoolingOptions(){

        final PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions
                .setCoreConnectionsPerHost(HostDistance.LOCAL, ioconfig.getDatastaxCoreConnectionsPerHost())
                .setMaxConnectionsPerHost(HostDistance.LOCAL, ioconfig.getDatastaxMaxConnectionsPerHost())
                .setMaxRequestsPerConnection(HostDistance.LOCAL, ioconfig.getDatastaxMaxRequestsPerConnection());
        return poolingOptions;
    }

    private void monitorConnection() {
        final MetricRegistry bfMetricsRegistry = com.rackspacecloud.blueflood.utils.Metrics.getRegistry();
        cluster.getMetrics().getRegistry().addListener(new com.codahale.metrics.MetricRegistryListener() {
            @Override
            public void onGaugeAdded(String name, Gauge<?> gauge) {
                bfMetricsRegistry.register(metricsPrefix + name, gauge);
            }

            @Override
            public void onGaugeRemoved(String name) {
                bfMetricsRegistry.remove(metricsPrefix + name);
            }

            @Override
            public void onCounterAdded(String name, Counter counter) {
                bfMetricsRegistry.register(metricsPrefix + name, counter);
            }

            @Override
            public void onCounterRemoved(String name) {
                bfMetricsRegistry.remove(metricsPrefix + name);
            }

            @Override
            public void onHistogramAdded(String name, Histogram histogram) {
                bfMetricsRegistry.register(metricsPrefix + name, histogram);
            }

            @Override
            public void onHistogramRemoved(String name) {
                bfMetricsRegistry.remove(metricsPrefix + name);
            }

            @Override
            public void onMeterAdded(String name, Meter meter) {
                bfMetricsRegistry.register(metricsPrefix + name, meter);
            }

            @Override
            public void onMeterRemoved(String name) {
                bfMetricsRegistry.remove(metricsPrefix + name);
            }

            @Override
            public void onTimerAdded(String name, Timer timer) {
                bfMetricsRegistry.register(metricsPrefix + name, timer);
            }

            @Override
            public void onTimerRemoved(String name) {
                bfMetricsRegistry.remove(metricsPrefix + name);
            }
        });
    }

    @Override
    public void close() { //Not to be used with time-series data.
        session.close();
        cluster.close();
    }

    /**
     * Gets the session for this instance. It's named oddly because of the existing {@link #getSession()}. Convert all
     * callers of that to use DatastaxIO.getInstance().getInstanceSession(), and then that method can go away, and this
     * can be renamed.
     */
    public Session getInstanceSession() {
        return session;
    }

    /**
     * Direct access to the singleton's Session because this is the way it was originally written. Prefer to {@link
     * #getInstance()} and get the session from there with {@link #getInstanceSession()}.
     */
    public static Session getSession() {
        return getInstance().session;
    }
}
