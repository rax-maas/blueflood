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
import java.util.List;
import java.util.Map;

public class ConfigurationTest {

    @Test
    public void testConfiguration() {
        Configuration config = Configuration.getInstance();
        Map<Object, Object> properties = config.getProperties();

        Assert.assertNotNull(properties);

        Assert.assertEquals("127.0.0.1:19180", config.getStringProperty(CoreConfig.CASSANDRA_HOSTS));
        System.setProperty("CASSANDRA_HOSTS", "127.0.0.2");
        Assert.assertEquals("127.0.0.2", config.getStringProperty(CoreConfig.CASSANDRA_HOSTS));

        Assert.assertEquals(60000, config.getIntegerProperty(CoreConfig.SCHEDULE_POLL_PERIOD));

    }

    @Test
    public void testInitWithBluefloodConfig() throws IOException {
        Configuration config = Configuration.getInstance();
        Assert.assertNull(config.getStringProperty("TEST_PROPERTY"));
        Assert.assertEquals("ALL", config.getStringProperty(CoreConfig.SHARDS));

        String configPath = new File("src/test/resources/bf-override-config.properties").getAbsolutePath();
        System.setProperty("blueflood.config", "file://" + configPath);
        config.init();

        Assert.assertEquals("foo", config.getStringProperty("TEST_PROPERTY"));
        Assert.assertEquals("NONE", config.getStringProperty(CoreConfig.SHARDS));
    }

    // list of strings

    @Test
    public void testMultipleCommaSeparatedItemsShouldYieldTheSameNumberOfElements() {

        String[] expected = { "a", "b", "c" };
        List<String> actual = Configuration.stringListFromString("a,b,c");

        Assert.assertArrayEquals(expected, actual.toArray());
    }

    @Test
    public void testWhitespaceBetweenElementsIsNotSignificant() {

        String[] expected = { "a", "b", "c" };
        List<String> actual = Configuration.stringListFromString("a,  b,c");

        Assert.assertArrayEquals(expected, actual.toArray());
    }

    @Test
    public void testLeadingWhitespaceIsKept() {

        String[] expected = { "   a", "b", "c" };
        List<String> actual = Configuration.stringListFromString("   a,b,c");

        Assert.assertArrayEquals(expected, actual.toArray());
    }

    @Test
    public void testTrailingWhitespaceIsKept() {

        String[] expected = { "a", "b", "c   " };
        List<String> actual = Configuration.stringListFromString("a,b,c   ");

        Assert.assertArrayEquals(expected, actual.toArray());
    }

    @Test
    public void testConsecutiveCommasDontProduceEmptyElements() {

        String[] expected = { "a", "b", "c" };
        List<String> actual = Configuration.stringListFromString("a,,,b,c");

        Assert.assertArrayEquals(expected, actual.toArray());
    }

    // boolean values

    @Test
    public void testNullShouldBeInterpretedAsBooleanFalse() {

        Assert.assertFalse(Configuration.booleanFromString(null));
    }

    @Test
    public void test_TRUE_ShouldBeInterpretedAsBooleanTrue() {

        Assert.assertTrue(Configuration.booleanFromString("TRUE"));
    }

    // override behavior

    @Test
    public void testSystemPropertiesOverrideConfigurationValues() {

        final String keyName = CoreConfig.MAX_CASSANDRA_CONNECTIONS.toString();

        Configuration config = Configuration.getInstance();

        try {
            Assert.assertEquals("75",
                    config.getStringProperty(CoreConfig.MAX_CASSANDRA_CONNECTIONS));

            System.setProperty(keyName, "something else");

            Assert.assertEquals("something else",
                    config.getStringProperty(CoreConfig.MAX_CASSANDRA_CONNECTIONS));
        } finally {
            System.clearProperty(keyName);
            config.clearProperty(keyName);
        }
    }

    // originals

    @Test
    public void testGettingValuesCreatesOriginals() {

        final String keyName = CoreConfig.ROLLUP_KEYSPACE.toString();

        try {
            // arrange
            Configuration config = Configuration.getInstance();

            Map<Object, Object> props = config.getAllProperties();

            // preconditions
            Assert.assertTrue(
                    "props should already have 'ROLLUP_KEYSPACE', because that's a default",
                    props.containsKey(keyName));
            Assert.assertFalse(
                    "props should not contain 'original.ROLLUP_KEYSPACE' yet",
                    props.containsKey("original." + keyName));

            // act
            System.setProperty(keyName, "some value");
            config.setProperty(keyName, "some other value");
            String value = config.getStringProperty(CoreConfig.ROLLUP_KEYSPACE);
            Assert.assertEquals("some value", value);

            // assert
            Map<Object, Object> props2 = config.getAllProperties();

            Assert.assertTrue(
                    "props2 should already have 'ROLLUP_KEYSPACE', because that's a default",
                    props2.containsKey(keyName));
            Assert.assertTrue(
                    "props2 should now contain 'original.ROLLUP_KEYSPACE'",
                    props2.containsKey("original." + keyName));

            Assert.assertEquals("some value", config.getStringProperty(CoreConfig.ROLLUP_KEYSPACE));
            Assert.assertEquals("some other value", config.getStringProperty("original." + keyName));

        } finally {
            System.clearProperty(keyName);
        }
    }

    // getProperties

    @Test
    public void testGetPropertiesDoesntWorkForDefaults() {

        Configuration config = Configuration.getInstance();

        Assert.assertEquals(
                "19180",
                config.getStringProperty(CoreConfig.DEFAULT_CASSANDRA_PORT));

        Map props = config.getProperties();

        Assert.assertFalse(props.containsKey(CoreConfig.DEFAULT_CASSANDRA_PORT.toString()));
    }

    @Test
    public void testGetPropertiesWorksForNonDefaults() {

        final String keyName = "abcdef";

        // arrange
        Configuration config = Configuration.getInstance();

        // preconditions
        Map props = config.getProperties();
        Assert.assertFalse(props.containsKey(keyName));

        // act
        config.setProperty(keyName, "123456");

        // assert
        Map props2 = config.getProperties();
        Assert.assertTrue(props2.containsKey(keyName));


    }
}
