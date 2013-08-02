package com.cloudkick.blueflood.service;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class ConfigurationTest {

    @Test
    public void testConfiguration() throws IOException {
        Configuration.init();
        Map<Object, Object> properties = Configuration.getProperties();

        Assert.assertNotNull(properties);
        Assert.assertTrue(properties.size() > 0);

        System.setProperty("SCRIBE_HOST", "127.0.0.2");
        Assert.assertEquals("127.0.0.2", Configuration.getStringProperty("SCRIBE_HOST"));

        Assert.assertEquals(600000, Configuration.getIntegerProperty("THRIFT_RPC_TIMEOUT"));

    }

    @Test
    public void testInitWithBluefloodConfig() throws IOException {
        Configuration.init();
        Map<Object, Object> properties = Configuration.getProperties();
        Assert.assertFalse(properties.containsKey("TEST_PROPERTY"));
        Assert.assertEquals("ALL", properties.get("SHARDS").toString());

        String configPath = new File("src/test/resources/bf-override-config.properties").getAbsolutePath();
        System.setProperty("blueflood.config", "file://" + configPath);
        Configuration.init();

        Assert.assertEquals("foo", properties.get("TEST_PROPERTY"));
        Assert.assertEquals("NONE", properties.get("SHARDS"));
    }
}
