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

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class ConfigurationTest {

    @Test
    public void testConfiguration() {
        Configuration config = Configuration.getInstance();
        Map<Object, Object> properties = config.getProperties();

        Assert.assertNotNull(properties);

        Assert.assertEquals("127.0.0.1:19180", config.getStringProperty(CoreConfigDefaults.CASSANDRA_HOSTS));
        System.setProperty("CASSANDRA_HOSTS", "127.0.0.2");
        Assert.assertEquals("127.0.0.2", config.getStringProperty(CoreConfigDefaults.CASSANDRA_HOSTS));

        Assert.assertEquals(60000, config.getIntegerProperty(CoreConfigDefaults.SCHEDULE_POLL_PERIOD));

    }

    @Test
    public void testInitWithBluefloodConfig() throws IOException {
        Configuration config = Configuration.getInstance();
        Assert.assertNull(config.getStringProperty("TEST_PROPERTY"));
        Assert.assertEquals("ALL", config.getStringProperty(CoreConfigDefaults.SHARDS));

        String configPath = new File("src/test/resources/bf-override-config.properties").getAbsolutePath();
        System.setProperty("blueflood.config", "file://" + configPath);
        config.init();

        Assert.assertEquals("foo", config.getStringProperty("TEST_PROPERTY"));
        Assert.assertEquals("NONE", config.getStringProperty(CoreConfigDefaults.SHARDS));
    }

    @Test
    public void testGetListProperty() {
        Configuration config = Configuration.getInstance();
        Assert.assertEquals(config.getStringProperty(CoreConfigDefaults.QUERY_MODULES), "");
        Assert.assertTrue(config.getListProperty(CoreConfigDefaults.QUERY_MODULES).isEmpty());
        System.setProperty("QUERY_MODULES", "a");
        Assert.assertEquals(config.getListProperty(CoreConfigDefaults.QUERY_MODULES).size(), 1);
        System.setProperty("QUERY_MODULES", "a,b , c");
        Assert.assertEquals(Arrays.asList("a","b","c"), config.getListProperty(CoreConfigDefaults.QUERY_MODULES));
    }
}
