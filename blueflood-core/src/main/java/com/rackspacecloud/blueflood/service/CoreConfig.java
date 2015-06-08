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
    EVENTS_MODULES(""),

    MAX_LOCATOR_FETCH_THREADS("2"),
    MAX_ROLLUP_READ_THREADS("20"),
    MAX_ROLLUP_WRITE_THREADS("5"),

    DISCOVERY_WRITER_MIN_THREADS("5"),
    DISCOVERY_WRITER_MAX_THREADS("10"),

    // Maximum threads that would access the cache concurrently
    META_CACHE_MAX_CONCURRENCY("50"),

    // Setting this to true will enable batched meta reads and writes from db (lazy loads and writes)
    META_CACHE_BATCHED_READS("false"),
    META_CACHE_BATCHED_WRITES("false"),

    META_CACHE_BATCHED_READS_THRESHOLD("100"), // how many rows to read at a time? (batch size)
    META_CACHE_BATCHED_READS_TIMER_MS("10"),  // how often to read? (batch timer) (read faster than writes)
    META_CACHE_BATCHED_READS_PIPELINE_DEPTH("10"), // how many outstanding batches? (1 thread per batch).
    META_CACHE_BATCHED_READS_QUEUE_SIZE("1000"),

    META_CACHE_BATCHED_WRITES_THRESHOLD("100"),  // how many meta columns to write at a time? (batch size)
    META_CACHE_BATCHED_WRITES_TIMER_MS("20"),   // how often to write? (batch timer)
    META_CACHE_BATCHED_WRITES_PIPELINE_DEPTH("10"), // how many outstanding batches? (1 thread per batch).
    META_CACHE_BATCHED_WRITES_QUEUE_SIZE("1000"),

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

    RIEMANN_HOST(""), //string: address of riemann server where events should be sent.
    RIEMANN_PORT("5555"),
    RIEMANN_PREFIX(""), //string: prefix metric names with this. useful for telling metrics from backfiller vs normal BF instance. (RIEMANN_LOCALHOST and RIEMANN_TAGS are better though)
    RIEMANN_LOCALHOST(""), //string: name of the server blueflood is running on.
    RIEMANN_TAGS(""), //comma-delimited list of strings: tags to append to metric events. ex- blueflood,ingest
    RIEMANN_SEPARATOR(""), //string: separator between metric name components. if set to "|" would result in: prefix|metric_name|rate_5m
    RIEMANN_TTL(""), //float: number of seconds until metric TTLs out of riemann's index

    GRAPHITE_HOST(""),
    GRAPHITE_PORT("2003"),
    GRAPHITE_PREFIX("unconfiguredNode.metrics."),

    INGEST_MODE("true"),
    ROLLUP_MODE("true"),
    QUERY_MODE("true"),

    METRICS_BATCH_WRITER_THREADS("50"),

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

    // Rollups repair on read
    REPAIR_ROLLUPS_ON_READ("true"),

    // valid options are: GEOMETRIC, LINEAR, and LESSTHANEQUAL
    GET_BY_POINTS_GRANULARITY_SELECTION("GEOMETRIC"),

    IMETRICS_WRITER("com.rackspacecloud.blueflood.io.AstyanaxMetricsWriter"),

    METADATA_CACHE_PERSISTENCE_ENABLED("false"),
    METADATA_CACHE_PERSISTENCE_PATH("/dev/null"),
    METADATA_CACHE_PERSISTENCE_PERIOD_MINS("10"),
    META_CACHE_RETENTION_IN_MINUTES("10"),
    
    // how long we typically wait to schedule a rollup.
    ROLLUP_DELAY_MILLIS("300000"),
    STRING_METRICS_DROPPED("false"),
    TENANTIDS_TO_KEEP(""),

    USE_ES_FOR_UNITS("false"),
    // Should at least be equal to the number of the netty worker threads, if http module is getting loaded
    ES_UNIT_THREADS("50"),
    ROLLUP_ON_READ_THREADS("50"),
    TURN_OFF_RR_MPLOT("false");

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
