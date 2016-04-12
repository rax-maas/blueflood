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

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.IOConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This class is a singleton that holds the necessary code that uses datastax
 * driver to make connections to Cassandra. All of the consumer code needing
 * to use datastax driver can use this class to get a {@link com.datastax.driver.core.Session}
 * object for the read/write statements to Cassandra.
 */
public class DatastaxIO {
    private static final DatastaxIO INSTANCE = new DatastaxIO();
    private static final Logger LOG = LoggerFactory.getLogger(DatastaxIO.class);
    private static final IOConfig ioconfig = IOConfig.singleton();

    private static  Cluster cluster;
    private static Session session;

//    private final static Meter hostMeter = utils.Metrics.meter(DatastaxIO.class, "Hosts Connected");
//    private final static Meter openConnectionsMeter = utils.Metrics.meter(DatastaxIO.class, "Total Open Connections");
//    private final static Meter inFlightQueriesMeter = utils.Metrics.meter(DatastaxIO.class, "Total InFlight Queries");
//    private final static Meter trashedConnectionsMeter = utils.Metrics.meter(DatastaxIO.class, "Total Trashed Connections");
//    private final static Meter maxLoadMeter = utils.Metrics.meter(DatastaxIO.class, "Maximum Load");

    static {
        connect();
        monitorConnection();
    }

    public static DatastaxIO singleton() { return INSTANCE; }

    private DatastaxIO() {
    }

    private static void connect() {
        Set<InetSocketAddress> dbHosts = ioconfig.getUniqueBinaryTransportHostsAsInetSocketAddresses();

        CodecRegistry codecRegistry = new CodecRegistry();

        cluster = Cluster.builder()
                .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().withLocalDc("datacenter1").build()))
                .withPoolingOptions(getPoolingOptions(dbHosts.size()))
                .withCodecRegistry(codecRegistry)
                .withSocketOptions(getSocketOptions())
                .addContactPointsWithPorts(dbHosts)
                .build();

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
        ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);
        scheduled.scheduleAtFixedRate(new Runnable() {
            public void run() {
                Session.State state = getSession().getState();
                Collection<Host> hosts = state.getConnectedHosts();
                int totalHosts = hosts.size();
                long totalOpenConnections = 0;
                long totalInFlightQueries = 0;
                long totalTrashedConnections = 0;
                long totalMaxLoad =0;
                for (Host host : hosts) {
                    int openConnections = state.getOpenConnections(host);
                    int inFlightQueries = state.getInFlightQueries(host);
                    int trashedConnections = state.getTrashedConnections(host);
                    int maxLoad = openConnections * 128;
                    totalOpenConnections += openConnections;
                    totalInFlightQueries += inFlightQueries;
                    totalTrashedConnections += trashedConnections;
                    totalMaxLoad += maxLoad;
                }

//                hostMeter.mark(totalHosts);
//                openConnectionsMeter.mark(totalOpenConnections);
//                inFlightQueriesMeter.mark(totalInFlightQueries);
//                trashedConnectionsMeter.mark(totalTrashedConnections);
//                maxLoadMeter.mark(totalMaxLoad);
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    public void close() { //Not to be used with time-series data.
        session.close();
        cluster.close();
    }

    public static Session getSession() {
        return session;
    }
}
