package com.rackspacecloud.blueflood.service;

import static junit.framework.Assert.*;

import org.junit.*;

import java.io.IOException;

public class BluefloodServiceStarterTest {

    @Before
    public void setUp() throws IOException {

        Configuration config = Configuration.getInstance();

        config.init();

        config.setProperty(CoreConfig.ZOOKEEPER_CLUSTER, "NONE");

        config.setProperty(CoreConfig.INGEST_MODE, "false");
        config.setProperty(CoreConfig.ROLLUP_MODE, "false");
        config.setProperty(CoreConfig.QUERY_MODE, "false");

        BluefloodServiceStarter.shouldInstantiateRestartGauge = false;
    }

    @Test
    public void testAllModesDisabled() {

        // given
        Configuration config = Configuration.getInstance();
        config.setProperty(CoreConfig.INGEST_MODE, "false");
        config.setProperty(CoreConfig.QUERY_MODE, "false");
        config.setProperty(CoreConfig.ROLLUP_MODE, "false");

        // when
        BluefloodServiceStarter.run();

        // then
        // no exception was thrown
    }

    @Test(expected = BluefloodServiceStarterException.class)
    public void testNoCassandraHostsFailsValidation() {

        // given
        Configuration config = Configuration.getInstance();
        config.setProperty(CoreConfig.CASSANDRA_HOSTS, "");

        // when
        BluefloodServiceStarter.validateCassandraHosts();

        // then
        // exception was thrown
    }

    @Test(expected = BluefloodServiceStarterException.class)
    public void testInvalidCassandraHostsFailsValidation() {

        // given
        Configuration config = Configuration.getInstance();
        config.setProperty(CoreConfig.CASSANDRA_HOSTS, "something");

        // when
        BluefloodServiceStarter.validateCassandraHosts();

        // then
        // exception was thrown
    }

    @Test(expected = BluefloodServiceStarterException.class)
    public void testCassandraHostWithoutPortFailsValidation() {

        // given
        Configuration config = Configuration.getInstance();
        config.setProperty(CoreConfig.CASSANDRA_HOSTS, "127.0.0.1");

        // when
        BluefloodServiceStarter.validateCassandraHosts();

        // then
        // exception was thrown
    }

    @Test(expected = BluefloodServiceStarterException.class)
    public void testIngestModeEnabledWithoutModules() {

        // given
        Configuration config = Configuration.getInstance();
        config.setProperty(CoreConfig.INGEST_MODE, "true");
        config.setProperty(CoreConfig.INGESTION_MODULES, "");

        // when
        BluefloodServiceStarter.run();

        // then
        // exception was thrown
    }

    @Test
    public void testIngestModeEnabledWithModules() {

        // given
        Configuration config = Configuration.getInstance();
        config.setProperty(CoreConfig.INGEST_MODE, "true");
        config.setProperty(CoreConfig.INGESTION_MODULES, "com.rackspacecloud.blueflood.service.DummyIngestionService");

        // when
        BluefloodServiceStarter.run();

        // then
        assertNotNull(DummyIngestionService.getInstances());
        assertEquals(1, DummyIngestionService.getInstances().size());
        assertNotNull(DummyIngestionService.getMostRecentInstance());

        assertTrue(DummyIngestionService.getMostRecentInstance().getStartServiceCalled());
        assertFalse(DummyIngestionService.getMostRecentInstance().getShutdownServiceCalled());
    }

    @Test(expected = BluefloodServiceStarterException.class)
    public void testQueryModeEnabledWithoutModules() {

        // given
        Configuration config = Configuration.getInstance();
        config.setProperty(CoreConfig.QUERY_MODE, "true");
        config.setProperty(CoreConfig.QUERY_MODULES, "");

        // when
        BluefloodServiceStarter.run();

        // then
        // exception was thrown
    }

    @Test
    public void testQueryModeEnabledWithDummyModule() {

        // given
        Configuration config = Configuration.getInstance();
        config.setProperty(CoreConfig.QUERY_MODE, "true");
        config.setProperty(CoreConfig.QUERY_MODULES, "com.rackspacecloud.blueflood.service.DummyQueryService");

        // when
        BluefloodServiceStarter.run();

        // then
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

        // when
        BluefloodServiceStarter.run();

        // then
        // no exception thrown
    }

    @Test
    public void testEventListenerServiceEnabledWithDummyModule() {

        // given
        Configuration config = Configuration.getInstance();
        config.setProperty(CoreConfig.EVENT_LISTENER_MODULES, "com.rackspacecloud.blueflood.service.DummyEventListenerService");

        // when
        BluefloodServiceStarter.run();

        // then
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
