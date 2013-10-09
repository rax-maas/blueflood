/*
 * Copyright 2013 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.io;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.retry.RetryNTimes;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.types.Locator;

import java.util.*;

public class AstyanaxIO {
    private static final AstyanaxContext<Keyspace> context;
    private static final Keyspace keyspace;
    protected static final ColumnFamily<Locator, Long> CF_METRICS_PREAGGREGATED = new ColumnFamily<Locator, Long>("metrics_preaggregated",
            LocatorSerializer.get(),
            LongSerializer.get());
    public static final ColumnFamily<Locator, Long> CF_METRICS_FULL = new ColumnFamily<Locator, Long>("metrics_full",
            LocatorSerializer.get(),
            LongSerializer.get());
    protected static final ColumnFamily<Locator, Long> CF_METRICS_5M = new ColumnFamily<Locator, Long>("metrics_5m",
            LocatorSerializer.get(),
            LongSerializer.get());
    protected static final ColumnFamily<Locator, Long> CF_METRICS_20M = new ColumnFamily<Locator, Long>("metrics_20m",
            LocatorSerializer.get(),
            LongSerializer.get());
    protected static final ColumnFamily<Locator, Long> CF_METRICS_60M = new ColumnFamily<Locator, Long>("metrics_60m",
            LocatorSerializer.get(),
            LongSerializer.get());
    protected static final ColumnFamily<Locator, Long> CF_METRICS_240M = new ColumnFamily<Locator, Long>("metrics_240m",
            LocatorSerializer.get(),
            LongSerializer.get());
    protected static final ColumnFamily<Locator, Long> CF_METRICS_1440M = new ColumnFamily<Locator, Long>("metrics_1440m",
            LocatorSerializer.get(),
            LongSerializer.get());
    protected static final ColumnFamily<Locator, String> CF_METRIC_METADATA = new ColumnFamily<Locator, String>("metrics_metadata",
            LocatorSerializer.get(),
            StringSerializer.get());
    protected static final ColumnFamily<Locator, Long> CF_METRICS_STRING = new ColumnFamily<Locator, Long>("metrics_string",
            LocatorSerializer.get(),
            LongSerializer.get());
    protected static final ColumnFamily<Long, Locator> CF_METRICS_LOCATOR = new ColumnFamily<Long, Locator>("metrics_locator",
            LongSerializer.get(),
            LocatorSerializer.get());
    protected static final ColumnFamily<Long, String> CF_METRICS_STATE = new ColumnFamily<Long, String>("metrics_state",
            LongSerializer.get(),
            StringSerializer.get());
    protected static final Map<String, ColumnFamily<Locator, Long>> CF_NAME_TO_CF;
    protected static final Map<ColumnFamily<Locator, Long>, Granularity> CF_TO_GRAN;

    static {
        context = createPreferredHostContext();
        context.start();
        keyspace = context.getEntity();
        Map<String, ColumnFamily<Locator, Long>> tempMap = new HashMap<String, ColumnFamily<Locator, Long>>();
        tempMap.put("metrics_full", CF_METRICS_FULL);
        tempMap.put("metrics_5m", CF_METRICS_5M);
        tempMap.put("metrics_20m", CF_METRICS_20M);
        tempMap.put("metrics_60m", CF_METRICS_60M);
        tempMap.put("metrics_240m", CF_METRICS_240M);
        tempMap.put("metrics_1440m", CF_METRICS_1440M);

        Map<ColumnFamily<Locator, Long>, Granularity> cfToGranMap = new HashMap<ColumnFamily<Locator, Long>, Granularity>();
        cfToGranMap.put(CF_METRICS_FULL, Granularity.FULL);
        cfToGranMap.put(CF_METRICS_STRING, Granularity.FULL);
        cfToGranMap.put(CF_METRICS_5M, Granularity.MIN_5);
        cfToGranMap.put(CF_METRICS_20M, Granularity.MIN_20);
        cfToGranMap.put(CF_METRICS_60M, Granularity.MIN_60);
        cfToGranMap.put(CF_METRICS_240M, Granularity.MIN_240);
        cfToGranMap.put(CF_METRICS_1440M, Granularity.MIN_1440);

        CF_NAME_TO_CF = Collections.unmodifiableMap(tempMap);
        CF_TO_GRAN = Collections.unmodifiableMap(cfToGranMap);
    }

    protected AstyanaxIO() {
    }

    private static AstyanaxContext<Keyspace> createCustomHostContext(AstyanaxConfigurationImpl configuration,
            ConnectionPoolConfigurationImpl connectionPoolConfiguration) {
        return new AstyanaxContext.Builder()
                .forCluster(Configuration.getStringProperty("CLUSTER_NAME"))
                .forKeyspace(Configuration.getStringProperty("ROLLUP_KEYSPACE"))
                .withAstyanaxConfiguration(configuration)
                .withConnectionPoolConfiguration(connectionPoolConfiguration)
                .withConnectionPoolMonitor(new InstrumentedConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());
    }

    private static AstyanaxContext<Keyspace> createPreferredHostContext() {
        return createCustomHostContext(createPreferredAstyanaxConfiguration(), createPreferredConnectionPoolConfiguration());
    }

    private static AstyanaxConfigurationImpl createPreferredAstyanaxConfiguration() {
        AstyanaxConfigurationImpl config = new AstyanaxConfigurationImpl()
                .setDiscoveryType(NodeDiscoveryType.NONE)
                .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN);

        int numRetries = Configuration.getIntegerProperty("CASSANDRA_MAX_RETRIES");
        if (numRetries > 0) {
            config.setRetryPolicy(new RetryNTimes(numRetries));
        }

        return config;
    }

    private static ConnectionPoolConfigurationImpl createPreferredConnectionPoolConfiguration() {
        int port = Configuration.getIntegerProperty("DEFAULT_CASSANDRA_PORT");
        Set<String> uniqueHosts = new HashSet<String>();
        Collections.addAll(uniqueHosts, Configuration.getStringProperty("CASSANDRA_HOSTS").split(","));
        int numHosts = uniqueHosts.size();
        int maxConns = Configuration.getIntegerProperty("MAX_CASSANDRA_CONNECTIONS");
        int connsPerHost = maxConns / numHosts + (maxConns % numHosts == 0 ? 0 : 1);
        // This timeout effectively results in waiting a maximum of (timeoutWhenExhausted / numHosts) on each Host
        int timeoutWhenExhausted = Configuration.getIntegerProperty("MAX_TIMEOUT_WHEN_EXHAUSTED");
        timeoutWhenExhausted = Math.max(timeoutWhenExhausted, 1 * numHosts); // Minimum of 1ms per host

        final ConnectionPoolConfigurationImpl connectionPoolConfiguration = new ConnectionPoolConfigurationImpl("MyConnectionPool")
                .setPort(port)
                .setInitConnsPerHost(connsPerHost)
                .setMaxConnsPerHost(connsPerHost)
                .setMaxBlockedThreadsPerHost(5)
                .setMaxTimeoutWhenExhausted(timeoutWhenExhausted)
                .setInitConnsPerHost(connsPerHost / 2)
                .setSeeds(Configuration.getStringProperty("CASSANDRA_HOSTS"));
        return connectionPoolConfiguration;
    }

    protected static Keyspace getKeyspace() {
        return keyspace;
    }

    public static Map<String, ColumnFamily<Locator, Long>> getColumnFamilyMapper() {
        return CF_NAME_TO_CF;
    }

    public static Map<ColumnFamily<Locator, Long>, Granularity> getCFToGranularityMapper() {
        return CF_TO_GRAN;
    }
}
