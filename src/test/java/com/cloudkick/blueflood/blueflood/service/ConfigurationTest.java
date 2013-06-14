package com.cloudkick.blueflood.service;

import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class ConfigurationTest extends TestCase {

    public void testConfiguration() throws IOException {
        Configuration.init();
        Map<Object, Object> properties = Configuration.getProperties();

        assertNotNull(properties);
        assertTrue(properties.size() > 0);

        System.setProperty("SCRIBE_HOST", "127.0.0.2");
        assertEquals("127.0.0.2", Configuration.getStringProperty("SCRIBE_HOST"));

        assertEquals(600000, Configuration.getIntegerProperty("THRIFT_RPC_TIMEOUT"));

    }

    public void testInitWithBluefloodConfig() throws IOException {
        Configuration.init();
        Map<Object, Object> properties = Configuration.getProperties();
        assertFalse(properties.containsKey("TEST_PROPERTY"));
        assertEquals("ALL", properties.get("SHARDS").toString());

        String configPath = new File("tests/test-data/bf-override-config.properties").getAbsolutePath();
        System.setProperty("blueflood.config", "file://" + configPath);
        Configuration.init();

        assertEquals("foo", properties.get("TEST_PROPERTY"));
        assertEquals("NONE", properties.get("SHARDS"));
    }
}
