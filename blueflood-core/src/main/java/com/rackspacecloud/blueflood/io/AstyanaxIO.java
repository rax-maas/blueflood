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
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.utils.TimeValue;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class AstyanaxIO {
    private static final AstyanaxContext<Keyspace> context;
    private static final Keyspace keyspace;
    
    public static final MetricColumnFamily CF_METRICS_FULL = new MetricColumnFamily("metrics_full", new TimeValue(1, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_5M = new MetricColumnFamily("metrics_5m", new TimeValue(2, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_20M = new MetricColumnFamily("metrics_20m", new TimeValue(3, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_60M = new MetricColumnFamily("metrics_60m", new TimeValue(31, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_240M = new MetricColumnFamily("metrics_240m", new TimeValue(60, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_1440M = new MetricColumnFamily("metrics_1440m", new TimeValue(365, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_STRING = new MetricColumnFamily("metrics_string", new TimeValue(365 * 3, TimeUnit.DAYS));
    
    public static final MetricColumnFamily CF_METRICS_PREAGGREGATED_FULL = new MetricColumnFamily("metrics_preaggregated_full", new TimeValue(1, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_PREAGGREGATED_5M = new MetricColumnFamily("metrics_preaggregated_5m", new TimeValue(2, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_PREAGGREGATED_20M = new MetricColumnFamily("metrics_preaggregated_20m", new TimeValue(3, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_PREAGGREGATED_60M = new MetricColumnFamily("metrics_preaggregated_60m", new TimeValue(31, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_PREAGGREGATED_240M = new MetricColumnFamily("metrics_preaggregated_240m", new TimeValue(60, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_PREAGGREGATED_1440M = new MetricColumnFamily("metrics_preaggregated_1440m", new TimeValue(365, TimeUnit.DAYS));

    public static final MetricColumnFamily CF_METRICS_HIST_FULL = CF_METRICS_FULL;
    public static final MetricColumnFamily CF_METRICS_HIST_5M = new MetricColumnFamily("metrics_histogram_5m", new TimeValue(2, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_HIST_20M = new MetricColumnFamily("metrics_histogram_20m", new TimeValue(3, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_HIST_60M = new MetricColumnFamily("metrics_histogram_60m", new TimeValue(31, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_HIST_240M = new MetricColumnFamily("metrics_histogram_240m", new TimeValue(60, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_HIST_1440M = new MetricColumnFamily("metrics_histogram_1440m", new TimeValue(365, TimeUnit.DAYS));

    private static final MetricColumnFamily[] METRIC_COLUMN_FAMILES = new MetricColumnFamily[] {
            CF_METRICS_FULL, CF_METRICS_5M, CF_METRICS_20M, CF_METRICS_60M, CF_METRICS_240M, CF_METRICS_1440M,
            CF_METRICS_PREAGGREGATED_FULL, CF_METRICS_PREAGGREGATED_5M, CF_METRICS_PREAGGREGATED_20M,
            CF_METRICS_PREAGGREGATED_60M, CF_METRICS_PREAGGREGATED_240M, CF_METRICS_PREAGGREGATED_1440M,
            CF_METRICS_HIST_FULL, CF_METRICS_HIST_5M, CF_METRICS_HIST_20M, CF_METRICS_HIST_60M,
            CF_METRICS_HIST_240M, CF_METRICS_HIST_1440M,
            CF_METRICS_STRING
    };
    
    public static final ColumnFamily<Locator, String> CF_METRIC_METADATA = new ColumnFamily<Locator, String>("metrics_metadata",
            LocatorSerializer.get(),
            StringSerializer.get());
    public static final ColumnFamily<Long, Locator> CF_METRICS_LOCATOR = new ColumnFamily<Long, Locator>("metrics_locator",
            LongSerializer.get(),
            LocatorSerializer.get());
    public static final ColumnFamily<Long, String> CF_METRICS_STATE = new ColumnFamily<Long, String>("metrics_state",
            LongSerializer.get(),
            StringSerializer.get());
    protected static final ColumnFamilyMapper CF_NAME_TO_CF;
    protected static final ColumnFamilyMapper PREAG_GRAN_TO_CF;
    protected static final Map<ColumnFamily<Locator, Long>, Granularity> CF_TO_GRAN;
    protected static final ColumnFamilyMapper HIST_GRAN_TO_CF;
    protected static final Configuration config = Configuration.getInstance();

    static {
        context = createPreferredHostContext();
        context.start();
        keyspace = context.getEntity();
        final Map<Granularity, MetricColumnFamily> columnFamilyMap = new HashMap<Granularity, MetricColumnFamily>();
        columnFamilyMap.put(Granularity.FULL, CF_METRICS_FULL);
        columnFamilyMap.put(Granularity.MIN_5, CF_METRICS_5M);
        columnFamilyMap.put(Granularity.MIN_20, CF_METRICS_20M);
        columnFamilyMap.put(Granularity.MIN_60, CF_METRICS_60M);
        columnFamilyMap.put(Granularity.MIN_240, CF_METRICS_240M);
        columnFamilyMap.put(Granularity.MIN_1440, CF_METRICS_1440M);
        
        final Map<Granularity, MetricColumnFamily> preagCFMap = new HashMap<Granularity, MetricColumnFamily>();
        preagCFMap.put(Granularity.FULL, CF_METRICS_PREAGGREGATED_FULL);
        preagCFMap.put(Granularity.MIN_5, CF_METRICS_PREAGGREGATED_5M);
        preagCFMap.put(Granularity.MIN_20, CF_METRICS_PREAGGREGATED_20M);
        preagCFMap.put(Granularity.MIN_60, CF_METRICS_PREAGGREGATED_60M);
        preagCFMap.put(Granularity.MIN_240, CF_METRICS_PREAGGREGATED_240M);
        preagCFMap.put(Granularity.MIN_1440, CF_METRICS_PREAGGREGATED_1440M);

        final Map<Granularity, MetricColumnFamily> histCFMap = new HashMap<Granularity, MetricColumnFamily>();
        histCFMap.put(Granularity.FULL, CF_METRICS_HIST_FULL);
        histCFMap.put(Granularity.MIN_5, CF_METRICS_HIST_5M);
        histCFMap.put(Granularity.MIN_20, CF_METRICS_HIST_20M);
        histCFMap.put(Granularity.MIN_60, CF_METRICS_HIST_60M);
        histCFMap.put(Granularity.MIN_240, CF_METRICS_HIST_240M);
        histCFMap.put(Granularity.MIN_1440, CF_METRICS_HIST_1440M);

        Map<ColumnFamily<Locator, Long>, Granularity> cfToGranMap = new HashMap<ColumnFamily<Locator, Long>, Granularity>();
        cfToGranMap.put(CF_METRICS_FULL, Granularity.FULL);
        cfToGranMap.put(CF_METRICS_STRING, Granularity.FULL);
        cfToGranMap.put(CF_METRICS_5M, Granularity.MIN_5);
        cfToGranMap.put(CF_METRICS_20M, Granularity.MIN_20);
        cfToGranMap.put(CF_METRICS_60M, Granularity.MIN_60);
        cfToGranMap.put(CF_METRICS_240M, Granularity.MIN_240);
        cfToGranMap.put(CF_METRICS_1440M, Granularity.MIN_1440);

        CF_NAME_TO_CF = new ColumnFamilyMapper() {
            @Override
            public MetricColumnFamily get(Granularity gran) {
                return columnFamilyMap.get(gran);
            }
        };
        PREAG_GRAN_TO_CF = new ColumnFamilyMapper() {
            @Override
            public MetricColumnFamily get(Granularity gran) {
                return preagCFMap.get(gran);
            }
        };
        HIST_GRAN_TO_CF = new ColumnFamilyMapper() {
            @Override
            public MetricColumnFamily get(Granularity gran) {
                return histCFMap.get(gran);
            }
        };
        CF_TO_GRAN = Collections.unmodifiableMap(cfToGranMap);
    }

    protected AstyanaxIO() {
    }

    private static AstyanaxContext<Keyspace> createCustomHostContext(AstyanaxConfigurationImpl configuration,
            ConnectionPoolConfigurationImpl connectionPoolConfiguration) {
        return new AstyanaxContext.Builder()
                .forCluster(config.getStringProperty(CoreConfig.CLUSTER_NAME))
                .forKeyspace(config.getStringProperty(CoreConfig.ROLLUP_KEYSPACE))
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
        Set<String> uniqueHosts = new HashSet<String>();
        Collections.addAll(uniqueHosts, config.getStringProperty(CoreConfig.CASSANDRA_HOSTS).split(","));
        int numHosts = uniqueHosts.size();
        int maxConns = config.getIntegerProperty(CoreConfig.MAX_CASSANDRA_CONNECTIONS);
        int timeout = config.getIntegerProperty(CoreConfig.CASSANDRA_REQUEST_TIMEOUT);

        int connsPerHost = maxConns / numHosts + (maxConns % numHosts == 0 ? 0 : 1);
        // This timeout effectively results in waiting a maximum of (timeoutWhenExhausted / numHosts) on each Host
        int timeoutWhenExhausted = config.getIntegerProperty(CoreConfig.MAX_TIMEOUT_WHEN_EXHAUSTED);
        timeoutWhenExhausted = Math.max(timeoutWhenExhausted, 1 * numHosts); // Minimum of 1ms per host

        final ConnectionPoolConfigurationImpl connectionPoolConfiguration = new ConnectionPoolConfigurationImpl("MyConnectionPool")
                .setPort(port)
                .setSocketTimeout(timeout)
                .setInitConnsPerHost(connsPerHost)
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

    public static ColumnFamilyMapper getColumnFamilyMapper() {
        return CF_NAME_TO_CF;
    }
    
    // todo: temporary. ColumnFamilyMapper should be modified to take StatType into account.
    public static ColumnFamilyMapper getPreagColumnFamilyMapper() {
        return PREAG_GRAN_TO_CF;
    }

    public static ColumnFamilyMapper getHistogramColumnFamilyMapper() {
        return HIST_GRAN_TO_CF;
    }

    // future versions will have get(Granularity, StatType).
    public interface ColumnFamilyMapper {
        public MetricColumnFamily get(Granularity gran);
    }
    
    // iterate over all column families that store metrics.
    public static Iterable<MetricColumnFamily> getMetricColumnFamilies() {
        return new Iterable<MetricColumnFamily>() {
            @Override
            public Iterator<MetricColumnFamily> iterator() {
                return new Iterator<MetricColumnFamily>() {
                    private int pos = 0;
                    @Override
                    public boolean hasNext() {
                        return pos < METRIC_COLUMN_FAMILES.length;
                    }

                    @Override
                    public MetricColumnFamily next() {
                        return METRIC_COLUMN_FAMILES[pos++];
                    }

                    @Override
                    public void remove() {
                        throw new NoSuchMethodError("Not implemented");
                    }
                };
            }
        };
    }
    
    public static class MetricColumnFamily extends ColumnFamily<Locator, Long>  {
        private final TimeValue ttl;
        
        public MetricColumnFamily(String name, TimeValue ttl) {
            super(name, LocatorSerializer.get(), LongSerializer.get());
            this.ttl = ttl;
        }
        
        public TimeValue getDefaultTTL() {
            return ttl;
        }
    }
}
