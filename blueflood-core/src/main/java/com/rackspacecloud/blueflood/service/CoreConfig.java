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

package com.rackspacecloud.blueflood.service;

/**
 * Default config values for blueflood-core. Also to be used for getting config key names.
 */
public enum CoreConfig implements ConfigDefaults {
    CASSANDRA_HOSTS("127.0.0.1:19180"),
    DEFAULT_CASSANDRA_PORT("19180"),
    // This number is only accurate if MAX_CASSANDRA_CONNECTIONS is evenly divisible by number of hosts
    MAX_CASSANDRA_CONNECTIONS("75"),

    ROLLUP_KEYSPACE("DATA"),
    CLUSTER_NAME("Test Cluster"),

    INGESTION_MODULES(""),
    QUERY_MODULES(""),
    DISCOVERY_MODULES(""),
    EVENT_LISTENER_MODULES(""),

    MAX_ROLLUP_READ_THREADS("20"),
    MAX_ROLLUP_WRITE_THREADS("5"),

    MAX_SCRIBE_WRITE_THREADS("50"),
    // Maximum timeout waiting on exhausted connection pools in milliseconds.
    // Maps directly to Astyanax's ConnectionPoolConfiguration.setMaxTimeoutWhenExhausted
    MAX_TIMEOUT_WHEN_EXHAUSTED("2000"),
    SCHEDULE_POLL_PERIOD("60000"),

    // Config refresh interval (If a new config is pushed out, we need to pick up the changes)
    // time is in milliseconds
    CONFIG_REFRESH_PERIOD("10000"),

    // this is a special string, or a comma list of integers. e.g.: "1,2,3,4"
    // valid shards are 0..127
    SHARDS("ALL"),

    // thread sleep times between shard push/pulls.
    SHARD_PUSH_PERIOD("2000"),
    SHARD_PULL_PERIOD("20000"),

    // blueflood uses zookeeper to acquire locks before working on shards
    ZOOKEEPER_CLUSTER("127.0.0.1:22181"),

    // 20 min
    SHARD_LOCK_HOLD_PERIOD_MS("1200000"),
    // 1 min
    SHARD_LOCK_DISINTERESTED_PERIOD_MS("60000"),
    // 2 min
    SHARD_LOCK_SCAVENGE_INTERVAL_MS("120000"),
    MAX_ZK_LOCKS_TO_ACQUIRE_PER_CYCLE("1"),

    INTERNAL_API_CLUSTER("127.0.0.1:50020,127.0.0.1:50020"),

    GRAPHITE_HOST(""),
    GRAPHITE_PORT("2003"),
    GRAPHITE_PREFIX("unconfiguredNode.metrics."),

    INGEST_MODE("true"),
    ROLLUP_MODE("true"),
    QUERY_MODE("true"),

    METRIC_BATCH_SIZE("100"),

    CASSANDRA_REQUEST_TIMEOUT("10000"),
    // set <= 0 to not retry
    CASSANDRA_MAX_RETRIES("5"),

    // v1.0 defaults to ','. This configuration option provides backwards compatibility.
    // Using legacy separators is deprecated as of 2.0 and will be removed in 3.0
    USE_LEGACY_METRIC_SEPARATOR("false"),

    ROLLUP_BATCH_MIN_SIZE("5"),
    ROLLUP_BATCH_MAX_SIZE("100"),

    ENABLE_HISTOGRAMS("false"),

    // Assume, for calculating granularity for GetByPoints queries, that data is sent at this interval.
    GET_BY_POINTS_ASSUME_INTERVAL("30000"),
    // valid options are: GEOMETRIC, LINEAR, and LESSTHANEQUAL
    GET_BY_POINTS_GRANULARITY_SELECTION("GEOMETRIC"),

    // TTL specification for various rollup & data types (specified in days)
    STRING_METRICS_TTL("365"),

    RAW_METRICS_TTL("7"), // 7 days
    BASIC_ROLLUPS_MIN5("30"), // 1 month
    BASIC_ROLLUPS_MIN20("60"), // 2 months
    BASIC_ROLLUPS_MIN60("90"), // 3 months
    BASIC_ROLLUPS_MIN240("180"), // 6 months
    BASIC_ROLLUPS_MIN1440("365"), // 1 year

    HIST_ROLLUPS_MIN5("30"), // 1 month
    HIST_ROLLUPS_MIN20("60"), // 2 months
    HIST_ROLLUPS_MIN60("90"), // 3 months
    HIST_ROLLUPS_MIN240("180"), // 6 months
    HIST_ROLLUPS_MIN1440("365"), // 1 year

    SET_ROLLUPS_FULL("7"), // 7 days
    SET_ROLLUPS_MIN5("30"), // 1 month
    SET_ROLLUPS_MIN20("60"), // 2 months
    SET_ROLLUPS_MIN60("90"), // 3 months
    SET_ROLLUPS_MIN240("180"), // 6 months
    SET_ROLLUPS_MIN1440("365"), // 1 year

    GAUGE_ROLLUPS_FULL("7"), // 7 days
    GAUGE_ROLLUPS_MIN5("30"), // 1 month
    GAUGE_ROLLUPS_MIN20("60"), // 2 months
    GAUGE_ROLLUPS_MIN60("90"), // 3 months
    GAUGE_ROLLUPS_MIN240("180"), // 6 months
    GAUGE_ROLLUPS_MIN1440("365"), // 1 year

    TIMER_ROLLUPS_FULL("7"), // 7 days
    TIMER_ROLLUPS_MIN5("30"), // 1 month
    TIMER_ROLLUPS_MIN20("60"), // 2 months
    TIMER_ROLLUPS_MIN60("90"), // 3 months
    TIMER_ROLLUPS_MIN240("180"), // 6 months
    TIMER_ROLLUPS_MIN1440("365"); // 1 year


    static {
        Configuration.getInstance().loadDefaults(CoreConfig.values());
    }
    private String defaultValue;
    private CoreConfig(String value) {
        this.defaultValue = value;
    }
    public String getDefaultValue() {
        return defaultValue;
    }
}
