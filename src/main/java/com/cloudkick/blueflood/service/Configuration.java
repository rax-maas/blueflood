package com.cloudkick.blueflood.service;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * java/conf/bf-dev.con has an exhaustive description of each configuration option.
 */
public class Configuration {
    private static final Properties props = new Properties();

    static {
        props.put("CASSANDRA_HOSTS", "127.0.0.1:19170");
        props.put("DEFAULT_CASSANDRA_PORT", "19170");
        // This number is only accurate if MAX_CASSANDRA_CONNECTIONS is evenly divisible by number of hosts
        props.put("MAX_CASSANDRA_CONNECTIONS", "70");

        props.put("ROLLUP_KEYSPACE", "DATA");
        props.put("CLUSTER_NAME", "Test Cluster");

        // blueflood receives metrics over the scribe interface.
        props.put("SCRIBE_HOST", "127.0.0.1");
        props.put("SCRIBE_PORT", "2466");

        // blueflood can receive metric over HTTP
        props.put("HTTP_INGESTION_PORT", "19000");

        // blueflood answers queries over the telescope interface.
        props.put("TELESCOPE_HOST", "127.0.0.1");
        props.put("TELESCOPE_PORT", "2467");

        // these settings apply to both thrift listeners that are created.
        props.put("THRIFT_RPC_TIMEOUT", "600000");
        props.put("THRIFT_LENGTH", "5242880");
        props.put("MAX_THREADS", "10");
        props.put("MIN_THREADS", "2");
        props.put("MAX_THRIFT_OVERFLOW_QUEUE_SIZE", "50");

        props.put("MAX_ROLLUP_THREADS", "20");
        // Maximum timeout waiting on exhausted connection pools in milliseconds.
        // Maps directly to Astyanax's ConnectionPoolConfiguration.setMaxTimeoutWhenExhausted
        props.put("MAX_TIMEOUT_WHEN_EXHAUSTED", "2000");
        props.put("SCHEDULE_POLL_PERIOD", "60000");

        // Config refresh interval (If a new config is pushed out, we need to pick up the changes)
        props.put("CONFIG_REFRESH_PERIOD", "10000"); // 10s

        // this is a special string, or a comma list of integers. e.g.: "1,2,3,4"
        // valid shards are 0..127
        props.put("SHARDS", "ALL");

        // thread sleep times between shard push/pulls.
        props.put("SHARD_PUSH_PERIOD", "2000");
        props.put("SHARD_PULL_PERIOD", "20000");

        // blueflood uses zookeeper to acquire locks before working on shards
        props.put("ZOOKEEPER_CLUSTER", "127.0.0.1:22181");

        props.put("MAX_SCRIBE_WRITE_THREADS", "50");

        props.put("SHARD_LOCK_HOLD_PERIOD_MS", "1200000"); // 20 min
        props.put("SHARD_LOCK_DISINTERESTED_PERIOD_MS", "60000"); // 1 min
        props.put("SHARD_LOCK_SCAVENGE_INTERVAL_MS", "120000"); // 2 min
        props.put("MAX_ZK_LOCKS_TO_ACQUIRE_PER_CYCLE", "1");

        props.put("INTERNAL_ELE_API_CLUSTER", "127.0.0.1:50020,127.0.0.1:50020");

        props.put("SERVICE_REGISTRY_USER", "");
        props.put("SERVICE_REGISTRY_API_KEY", "");
        props.put("SERVICE_REGISTRY_ENABLED", "false");

        props.put("STATSD_HOST", "127.0.0.1");
        props.put("STATSD_PORT", "8125");
        props.put("GRAPHITE_HOST", "");
        props.put("GRAPHITE_PORT", "2003");
        props.put("GRAPHITE_PREFIX", "unconfiguredNode.ele.gauges.");
        
        props.put("INGESTION_MODE", "true");
        props.put("ROLLUP_MODE", "true");
        props.put("QUERY_MODE", "true");
    }

    public static void init() throws IOException {
        // load the configuration.
        String configStr = System.getProperty("blueflood.config");
        if (configStr != null) {
            URL configUrl = new URL(configStr);
            Properties props = new Properties();
            props.load(configUrl.openStream());
            Configuration.init(props);
        }
    }

    public static void init(Properties p) {
        props.putAll(p);
    }

    public static Map<Object,Object> getProperties() {
        return Collections.unmodifiableMap(props);
    }

    public static String getStringProperty(String name) {
        if (System.getProperty(name) != null && !props.containsKey("original." + name)) {
            props.put("original." + name, props.get(name));
            props.put(name, System.getProperty(name));
        }
        return props.getProperty(name);
    }

    public static int getIntegerProperty(String name) {
        return Integer.parseInt(getStringProperty(name));
    }

    public static float getFloatProperty(String name) {
        return Float.parseFloat(getStringProperty(name));
    }

    public static long getLongProperty(String name) {
        return Long.parseLong(getStringProperty(name));
    }

    public static boolean getBooleanProperty(String name) {
        return getStringProperty(name).equalsIgnoreCase("true");
    }
}

