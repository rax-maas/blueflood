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
public class DatastaxIO {
    private static final Logger LOG = LoggerFactory.getLogger(DatastaxIO.class);
    private static final IOConfig ioconfig = IOConfig.singleton();
    private static final String metricsPrefix = "datastax.";

    private static  Cluster cluster;
    private static Session session;

    static {
        connect();
        monitorConnection();
    }

    private DatastaxIO() {
    }

    private static void connect() {
        Set<InetSocketAddress> dbHosts = ioconfig.getUniqueBinaryTransportHostsAsInetSocketAddresses();

        CodecRegistry codecRegistry = new CodecRegistry();

        cluster = Cluster.builder()
                .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().withLocalDc("datacenter1").build(), false))
                .withPoolingOptions(getPoolingOptions(dbHosts.size()))
                .withCodecRegistry(codecRegistry)
                .withSocketOptions(getSocketOptions())
                .addContactPointsWithPorts(dbHosts)
                .build();

        QueryLogger queryLogger = QueryLogger.builder()
                .withConstantThreshold(5000)
                .build();

        cluster.register(queryLogger);

        if ( LOG.isDebugEnabled() ) {
            logDebugConnectionInfo();
        }

        try {
            session = cluster.connect( CassandraModel.QUOTED_KEYSPACE );
        }
        catch (NoHostAvailableException e){
            // TODO: figure out how to bubble this up
            throw new RuntimeException(e);
        }
    }

    private static void logDebugConnectionInfo() {
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

    private static SocketOptions getSocketOptions() {
        final SocketOptions socketOptions = new SocketOptions();
        socketOptions.setConnectTimeoutMillis(ioconfig.getRequestTimeout())
                     .setReadTimeoutMillis(ioconfig.getRequestTimeout());
        return socketOptions;
    }

    private static PoolingOptions getPoolingOptions(int numHosts){

        final PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions
                .setCoreConnectionsPerHost(HostDistance.LOCAL, ioconfig.getInitialConn())
                .setMaxConnectionsPerHost(HostDistance.LOCAL, ioconfig.getMaxConnPerHost(numHosts));
        return poolingOptions;
    }

    private static void monitorConnection() {
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

    public void close() { //Not to be used with time-series data.
        session.close();
        cluster.close();
    }

    public static Session getSession() {
        return session;
    }
}
