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

package com.rackspacecloud.blueflood.io.astyanax;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.retry.RetryNTimes;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.BooleanSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.IOConfig;
import com.rackspacecloud.blueflood.io.InstrumentedConnectionPoolMonitor;
import com.rackspacecloud.blueflood.io.serializers.Serializers;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.DataType;
import com.rackspacecloud.blueflood.types.RollupType;

import java.util.*;

/**
 * This class is a singleton that holds the necessary code that uses Astyanax
 * driver to make connections to Cassandra. All of the consumer code needing
 * to use datastax driver can use this class to get a {@link com.netflix.astyanax.Keyspace}
 * and/or {@link com.netflix.astyanax.AstyanaxContext}
 * object for the read/write statements to Cassandra.
 */
public class AstyanaxIO {
    private static final AstyanaxContext<Keyspace> context;
    private static final Keyspace keyspace;
    protected static final Configuration config = Configuration.getInstance();
    private static final IOConfig ioconfig = IOConfig.singleton();

    private static AstyanaxIO INSTANCE = new AstyanaxIO();

    static {
        context = createPreferredHostContext();
        context.start();
        keyspace = context.getEntity();
    }

    public static AstyanaxIO singleton() { return INSTANCE; }

    protected AstyanaxIO() {
    }

    private static AstyanaxContext<Keyspace> createCustomHostContext(AstyanaxConfigurationImpl configuration,
            ConnectionPoolConfigurationImpl connectionPoolConfiguration) {
        return new AstyanaxContext.Builder()
                .forCluster(CassandraModel.CLUSTER)
                .forKeyspace(CassandraModel.KEYSPACE)
                .withAstyanaxConfiguration(configuration)
                .withConnectionPoolConfiguration(connectionPoolConfiguration)
                .withConnectionPoolMonitor(new InstrumentedConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());
    }

    private static AstyanaxContext<Keyspace> createPreferredHostContext() {
        return createCustomHostContext(createPreferredAstyanaxConfiguration(), createPreferredConnectionPoolConfiguration());
    }

    private static AstyanaxConfigurationImpl createPreferredAstyanaxConfiguration() {
        AstyanaxConfigurationImpl astyconfig = new AstyanaxConfigurationImpl()
                .setDiscoveryType(NodeDiscoveryType.NONE)
                .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN);

        int numRetries = config.getIntegerProperty(CoreConfig.CASSANDRA_MAX_RETRIES);
        if (numRetries > 0) {
            astyconfig.setRetryPolicy(new RetryNTimes(numRetries));
        }

        return astyconfig;
    }

    private static ConnectionPoolConfigurationImpl createPreferredConnectionPoolConfiguration() {
        int port = config.getIntegerProperty(CoreConfig.DEFAULT_CASSANDRA_PORT);
        Set<String> uniqueHosts = ioconfig.getUniqueHosts();
        int numHosts = uniqueHosts.size();
        int connsPerHost = ioconfig.getMaxConnPerHost(numHosts);

        int timeout = config.getIntegerProperty(CoreConfig.CASSANDRA_REQUEST_TIMEOUT);

        // This timeout effectively results in waiting a maximum of (timeoutWhenExhausted / numHosts) on each Host
        int timeoutWhenExhausted = config.getIntegerProperty(CoreConfig.MAX_TIMEOUT_WHEN_EXHAUSTED);
        timeoutWhenExhausted = Math.max(timeoutWhenExhausted, 1 * numHosts); // Minimum of 1ms per host

        final ConnectionPoolConfigurationImpl connectionPoolConfiguration = new ConnectionPoolConfigurationImpl("MyConnectionPool")
                .setPort(port)
                .setSocketTimeout(timeout)
                .setMaxConnsPerHost(connsPerHost)
                .setMaxBlockedThreadsPerHost(5)
                .setMaxTimeoutWhenExhausted(timeoutWhenExhausted)
                .setInitConnsPerHost(connsPerHost / 2)
                .setSeeds(config.getStringProperty(CoreConfig.CASSANDRA_HOSTS));
        return connectionPoolConfiguration;
    }

    protected static Keyspace getKeyspace() {
        return keyspace;
    }

    protected AbstractSerializer serializerFor(RollupType rollupType, DataType dataType, Granularity gran) {
        if (rollupType == null) {
            rollupType = RollupType.BF_BASIC;
        }

        if (dataType == null) {
            dataType = DataType.NUMERIC;
        }

        if (dataType.equals(DataType.STRING)) {
            return StringSerializer.get();
        } else if (dataType.equals(DataType.BOOLEAN)) {
            return BooleanSerializer.get();
        } else {
            return Serializers.serializerFor(RollupType.classOf(rollupType, gran));
        }
    }
}
