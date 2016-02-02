package com.rackspacecloud.blueflood.service;

import static junit.framework.Assert.*;

import org.junit.*;

import java.io.IOException;

public class BluefloodServiceStarterTest {

    @Before
    public void setUp() throws IOException {

        Configuration config = Configuration.getInstance();

        config.init();

        // take from demo/local/config/blueflood.conf
        config.setProperty(CoreConfig.CASSANDRA_HOSTS, "127.0.0.1:9160");
        config.setProperty(CoreConfig.DEFAULT_CASSANDRA_PORT, "9160");
        config.setProperty(CoreConfig.MAX_CASSANDRA_CONNECTIONS, "70");
        config.setProperty(CoreConfig.ROLLUP_KEYSPACE, "DATA");
        config.setProperty(CoreConfig.CLUSTER_NAME, "Test Cluster");
        config.setProperty(CoreConfig.QUERY_MODULES, "com.rackspacecloud.blueflood.service.HttpQueryService");
        config.setProperty(CoreConfig.INGESTION_MODULES, "com.rackspacecloud.blueflood.service.HttpIngestionService");
        config.setProperty(CoreConfig.DISCOVERY_MODULES, "com.rackspacecloud.blueflood.io.ElasticIO");
        config.setProperty(CoreConfig.EVENTS_MODULES, "com.rackspacecloud.blueflood.io.EventElasticSearchIO");
        config.setProperty(CoreConfig.ENUMS_DISCOVERY_MODULES, "com.rackspacecloud.blueflood.io.EnumElasticIO");
        config.setProperty(CoreConfig.MAX_ROLLUP_READ_THREADS, "20");
        config.setProperty(CoreConfig.MAX_ROLLUP_WRITE_THREADS, "20");
        config.setProperty(CoreConfig.MAX_TIMEOUT_WHEN_EXHAUSTED, "2000");
        config.setProperty(CoreConfig.SCHEDULE_POLL_PERIOD, "60000");
        config.setProperty(CoreConfig.CONFIG_REFRESH_PERIOD, "10000");
        config.setProperty(CoreConfig.SHARDS, "ALL");
        config.setProperty(CoreConfig.SHARD_PUSH_PERIOD, "2000");
        config.setProperty(CoreConfig.SHARD_PULL_PERIOD, "20000");
        config.setProperty(CoreConfig.ZOOKEEPER_CLUSTER, "NONE");
        config.setProperty(CoreConfig.SHARD_LOCK_HOLD_PERIOD_MS, "1200000");
        config.setProperty(CoreConfig.SHARD_LOCK_DISINTERESTED_PERIOD_MS, "60000");
        config.setProperty(CoreConfig.SHARD_LOCK_SCAVENGE_INTERVAL_MS, "120000");
        config.setProperty(CoreConfig.MAX_ZK_LOCKS_TO_ACQUIRE_PER_CYCLE, "1");
        config.setProperty(CoreConfig.INTERNAL_API_CLUSTER, "127.0.0.1:50020,127.0.0.1:50020");
        config.setProperty(CoreConfig.GRAPHITE_HOST, "");
        config.setProperty(CoreConfig.GRAPHITE_PORT, "2003");
        config.setProperty(CoreConfig.GRAPHITE_PREFIX, "unconfiguredNode.metrics.");
        config.setProperty(CoreConfig.INGEST_MODE, "false");
        config.setProperty(CoreConfig.ROLLUP_MODE, "false");
        config.setProperty(CoreConfig.QUERY_MODE, "false");
        config.setProperty(CoreConfig.CASSANDRA_MAX_RETRIES, "5");
        config.setProperty("ELASTICSEARCH_HOSTS", "localhost:9300");
        config.setProperty("ELASTICSEARCH_CLUSTERNAME", "elasticsearch");

        BluefloodServiceStarter.ThrowInsteadOfExit = true;
        BluefloodServiceStarter.SkipInstantiateRestartGauge = true;
    }

    @Test
    public void testStartup() {
        String[] args = new String[0];
        BluefloodServiceStarter.main(args);

        assertNotNull(args);
    }

    @Test
    public void testNoCassandraHostsFailsValidation() {

        // given
        Configuration config = Configuration.getInstance();
        config.setProperty(CoreConfig.CASSANDRA_HOSTS, "");

        // when
        BluefloodServiceStarterException ex = null;
        try {
            BluefloodServiceStarter.validateCassandraHosts();
        } catch (BluefloodServiceStarterException e) {
            ex = e;
        }

        // then
        assertNotNull(ex);
        assertEquals(-1, ex.getStatus());
    }

    @Test
    public void testInvalidCassandraHostsFailsValidation() {

        // given
        Configuration config = Configuration.getInstance();
        config.setProperty(CoreConfig.CASSANDRA_HOSTS, "something");

        // when
        BluefloodServiceStarterException ex = null;
        try {
            BluefloodServiceStarter.validateCassandraHosts();
        } catch (BluefloodServiceStarterException e) {
            ex = e;
        }

        // then
        assertNotNull(ex);
        assertEquals(-1, ex.getStatus());
    }

    @Test
    public void testCassandraHostWithoutPortFailsValidation() {

        // given
        Configuration config = Configuration.getInstance();
        config.setProperty(CoreConfig.CASSANDRA_HOSTS, "127.0.0.1");

        // when
        BluefloodServiceStarterException ex = null;
        try {
            BluefloodServiceStarter.validateCassandraHosts();
        } catch (BluefloodServiceStarterException e) {
            ex = e;
        }

        // then
        assertNotNull(ex);
        assertEquals(-1, ex.getStatus());
    }

    @Test
    public void testIngestModeEnabledWithoutModules() {

        // given
        Configuration config = Configuration.getInstance();
        config.setProperty(CoreConfig.INGEST_MODE, "true");
        config.setProperty(CoreConfig.INGESTION_MODULES, "");
        String[] args = new String[0];

        // when
        BluefloodServiceStarterException ex = null;
        try {
            BluefloodServiceStarter.main(args);
        } catch (BluefloodServiceStarterException e) {
            ex = e;
        }

        // then
        assertNotNull(ex);
        assertEquals(1, ex.getStatus());
    }

    @Test
    public void testIngestModeEnabledWithModules() {

        // given
        Configuration config = Configuration.getInstance();
        config.setProperty(CoreConfig.INGEST_MODE, "true");
        config.setProperty(CoreConfig.INGESTION_MODULES, "com.rackspacecloud.blueflood.service.DummyIngestionService");
        String[] args = new String[0];

        // when
        BluefloodServiceStarterException ex = null;
        try {
            BluefloodServiceStarter.main(args);
        } catch (BluefloodServiceStarterException e) {
            ex = e;
        }

        // then
        assertNull(ex);

        assertNotNull(DummyIngestionService.getInstances());
        assertEquals(1, DummyIngestionService.getInstances().size());
        assertNotNull(DummyIngestionService.getMostRecentInstance());

        assertTrue(DummyIngestionService.getMostRecentInstance().getStartServiceCalled());
        assertFalse(DummyIngestionService.getMostRecentInstance().getShutdownServiceCalled());
    }

    @Test
    public void testQueryModeEnabledWithoutModules() {

        // given
        Configuration config = Configuration.getInstance();
        config.setProperty(CoreConfig.QUERY_MODE, "true");
        config.setProperty(CoreConfig.QUERY_MODULES, "");
        String[] args = new String[0];

        // when
        BluefloodServiceStarterException ex = null;
        try {
            BluefloodServiceStarter.main(args);
        } catch (BluefloodServiceStarterException e) {
            ex = e;
        }

        // then
        assertNotNull(ex);
        assertEquals(1, ex.getStatus());
    }

    @Test
    public void testQueryModeEnabledWithDummyModule() {

        // given
        Configuration config = Configuration.getInstance();
        config.setProperty(CoreConfig.QUERY_MODE, "true");
        config.setProperty(CoreConfig.QUERY_MODULES, "com.rackspacecloud.blueflood.service.DummyQueryService");
        String[] args = new String[0];

        // when
        BluefloodServiceStarterException ex = null;
        try {
            BluefloodServiceStarter.main(args);
        } catch (BluefloodServiceStarterException e) {
            ex = e;
        }

        // then
        assertNull(ex);

        assertNotNull(DummyQueryService.getInstances());
        assertEquals(1, DummyQueryService.getInstances().size());
        assertNotNull(DummyQueryService.getMostRecentInstance());

        assertTrue(DummyQueryService.getMostRecentInstance().getStartServiceCalled());
    }

    @Test
    public void testRollupModeEnabled() {

        // given
        Configuration config = Configuration.getInstance();
        config.setProperty(CoreConfig.ROLLUP_MODE, "true");
        String[] args = new String[0];

        // when
        BluefloodServiceStarterException ex = null;
        try {
            BluefloodServiceStarter.main(args);
        } catch (BluefloodServiceStarterException e) {
            ex = e;
        }

        // then
        assertNull(ex);
    }

    @Test
    public void testEventListenerServiceEnabledWithDummyModule() {

        // given
        Configuration config = Configuration.getInstance();
        config.setProperty(CoreConfig.EVENT_LISTENER_MODULES, "com.rackspacecloud.blueflood.service.DummyEventListenerService");
        String[] args = new String[0];

        // when
        BluefloodServiceStarterException ex = null;
        try {
            BluefloodServiceStarter.main(args);
        } catch (BluefloodServiceStarterException e) {
            ex = e;
        }

        // then
        assertNull(ex);

        assertNotNull(DummyEventListenerService.getInstances());
        assertEquals(1, DummyEventListenerService.getInstances().size());
        assertNotNull(DummyEventListenerService.getMostRecentInstance());

        assertTrue(DummyEventListenerService.getMostRecentInstance().getStartServiceCalled());
    }

    @After
    public void tearDown() throws IOException {
        Configuration config = Configuration.getInstance();
        config.init();
    }
}
