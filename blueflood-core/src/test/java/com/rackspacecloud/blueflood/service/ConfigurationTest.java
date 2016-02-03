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

import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ConfigurationTest {

    @Before
    public void setup() throws IOException {
        Configuration.getInstance().init();
    }

    @After
    public void tearDown() throws IOException {

        // this resets the configuration after each test method invocation
        Configuration.getInstance().init();
    }

    @Test
    public void testConfiguration() {
        try {
            Configuration config = Configuration.getInstance();
            Map<Object, Object> properties = config.getProperties();

            Assert.assertNotNull(properties);

            Assert.assertEquals("127.0.0.1:19180", config.getStringProperty(CoreConfig.CASSANDRA_HOSTS));
            System.setProperty("CASSANDRA_HOSTS", "127.0.0.2");
            Assert.assertEquals("127.0.0.2", config.getStringProperty(CoreConfig.CASSANDRA_HOSTS));

            Assert.assertEquals(60000, config.getIntegerProperty(CoreConfig.SCHEDULE_POLL_PERIOD));
        } finally {
            System.clearProperty("CASSANDRA_HOSTS");
        }
    }

    @Test
    public void testInitWithBluefloodConfig() throws IOException {
        try {
            Configuration config = Configuration.getInstance();
            Assert.assertNull(config.getStringProperty("TEST_PROPERTY"));
            Assert.assertEquals("ALL", config.getStringProperty(CoreConfig.SHARDS));

            String configPath = new File("src/test/resources/bf-override-config.properties").getAbsolutePath();
            System.setProperty("blueflood.config", "file://" + configPath);
            config.init();

            Assert.assertEquals("foo", config.getStringProperty("TEST_PROPERTY"));
            Assert.assertEquals("NONE", config.getStringProperty(CoreConfig.SHARDS));
        } finally {
            System.clearProperty("blueflood.config");
        }
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

    // integer values

    @Test
    public void testIntegerOneShouldBeInterpretedAsOne() {
        Assert.assertEquals(1, Configuration.intFromString("1"));
    }

    @Test(expected=NumberFormatException.class)
    public void testIntegerLeadingWhitespaceShouldBeIgnored() {
        int value = Configuration.intFromString("   1");
    }

    @Test(expected=NumberFormatException.class)
    public void testIntegerTrailingWhitespaceShouldBeIgnored() {
        int value = Configuration.intFromString("1   ");
    }

    // long values

    @Test
    public void testLongOneShouldBeInterpretedAsOne() {
        Assert.assertEquals(1L, Configuration.longFromString("1"));
    }

    @Test(expected=NumberFormatException.class)
    public void testLongLeadingWhitespaceShouldBeRejected() {
        long value = Configuration.longFromString("   1");
    }

    @Test(expected=NumberFormatException.class)
    public void testLongTrailingWhitespaceShouldBeRejected() {
        long value = Configuration.longFromString("1   ");
    }

    // float values

    @Test
    public void testFloatOneShouldBeInterpretedAsOne() {
        Assert.assertEquals(1.0f, Configuration.floatFromString("1"), 0.00001f);
    }

    @Test
    public void testFloatExtendedFormat() {
        Assert.assertEquals(-1100.0f, Configuration.floatFromString("-1.1e3"), 0.00001f);
    }

    @Test(expected=NumberFormatException.class)
    public void testFloatExtendedFormatSpaceBeforeDotIsInvalid() {
        Assert.assertEquals(-1100.0f, Configuration.floatFromString("-1 .1e3"), 0.00001f);
    }

    @Test(expected=NumberFormatException.class)
    public void testFloatExtendedFormatSpaceAfterDotIsInvalid() {
        Assert.assertEquals(-1100.0f, Configuration.floatFromString("-1. 1e3"), 0.00001f);
    }

    @Test(expected=NumberFormatException.class)
    public void testFloatExtendedFormatSpaceBeforeExponentMarkerIsInvalid() {
        Assert.assertEquals(-1100.0f, Configuration.floatFromString("-1.1 e3"), 0.00001f);
    }

    @Test(expected=NumberFormatException.class)
    public void testFloatExtendedFormatSpaceAfterExponentMarkerIsInvalid() {
        Assert.assertEquals(-1100.0f, Configuration.floatFromString("-1.1e 3"), 0.00001f);
    }

    @Test
    public void testFloatLeadingWhitespaceShouldBeIgnored() {
        Assert.assertEquals(1.0f, Configuration.floatFromString("   1"), 0.00001f);
    }

    @Test
    public void testFloatTrailingWhitespaceShouldBeIgnored() {
        Assert.assertEquals(1.0f, Configuration.floatFromString("1   "), 0.00001f);
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

    // originals, a.k.a. backup keys

    @Test
    public void testGettingValuesCreatesOriginals() {

        final String keyName = CoreConfig.ROLLUP_KEYSPACE.toString();

        // The behavior of getStringProperty() when it creates new 'original.*'
        // entries is very convoluted. It requires:
        //  1. A system property named $NAME exists
        //  2. A property named "original.$NAME" does NOT exist in the props object
        //  3. A property named $NAME does exist in the props object
        // Hence the calls to System.setProperty and config.setProperty below
        // with different values. That way, we create the 'original.*' entry
        // from the intended source and can test it.

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

    @Test
    public void testWhenNoSyspropExistsAndTheKeyIsntPresentInConfigAndTheBackupKeyIsntPresentThenReturnValueShouldBeNullAndNoBackupCreated() {

        // arrange
        final String keyName = "some-key-name";
        final String keyName2 = "original." + keyName;
        Configuration config = Configuration.getInstance();

        try {
            // precondition
            Assert.assertFalse(config.containsKey(keyName));
            Assert.assertFalse(config.containsKey(keyName2));

            // act
            String value = config.getStringProperty(keyName);

            // assert

            // return value is null
            Assert.assertNull(value);

            // the key is not present
            Assert.assertFalse(config.containsKey(keyName));

            // the backup key is not present
            Assert.assertFalse(config.containsKey(keyName2));

        } finally {
            config.clearProperty(keyName);
            config.clearProperty(keyName2);
            System.clearProperty(keyName);
        }
    }

    @Test
    public void testWhenNoSyspropExistsAndTheKeyIsPresentInConfigAndTheBackupKeyIsntPresentThenReturnValueShouldBeThePreviouslySetValueAndNoBackupCreated() {

        // arrange
        final String keyName = "some-key-name";
        final String keyName2 = "original." + keyName;
        Configuration config = Configuration.getInstance();

        try {
            // precondition
            Assert.assertFalse(config.containsKey(keyName));
            Assert.assertFalse(config.containsKey(keyName2));

            // act
            config.setProperty(keyName, "some value");
            String value = config.getStringProperty(keyName);

            // assert

            // return value is previously-set value
            Assert.assertEquals("some value", value);

            // key is present and its value is equal to what we set it to
            Assert.assertTrue(config.containsKey(keyName));
            Assert.assertEquals("some value", config.getRawStringProperty(keyName));

            //backup key is not present
            Assert.assertFalse(config.containsKey(keyName2));

        } finally {
            config.clearProperty(keyName);
            config.clearProperty(keyName2);
            System.clearProperty(keyName);
        }
    }

    @Test
    public void testWhenSyspropExistsAndTheKeyIsntPresentInConfigAndTheBackupKeyIsntPresentThenReturnValueShouldBeSyspropValueAndNoBackupCreated() {

        // arrange
        final String keyName = "some-key-name";
        final String keyName2 = "original." + keyName;
        Configuration config = Configuration.getInstance();

        try {
            // precondition
            Assert.assertFalse(config.containsKey(keyName));
            Assert.assertFalse(config.containsKey(keyName2));

            // act
            System.setProperty(keyName, "some value");
            String value = config.getStringProperty(keyName);

            // assert

            // return value is system property value
            Assert.assertEquals("some value", value);

            // the key has been added and its value is that of the sysprop
            Assert.assertTrue(config.containsKey(keyName));
            Assert.assertEquals("some value", config.getRawStringProperty(keyName));

            // the backup key has not been added
            Assert.assertFalse(config.containsKey(keyName2));

        } finally {
            config.clearProperty(keyName);
            config.clearProperty(keyName2);
            System.clearProperty(keyName);
        }
    }

    @Test
    public void testWhenSyspropExistsAndTheKeyIsPresentInConfigAndTheBackupKeyIsntPresentThenReturnValueShouldBeSyspropValueAndNoBackupCreated() {

        // arrange
        final String keyName = "some-key-name";
        final String keyName2 = "original." + keyName;
        Configuration config = Configuration.getInstance();

        try {
            // precondition
            Assert.assertFalse(config.containsKey(keyName));
            Assert.assertFalse(config.containsKey(keyName2));

            // act
            System.setProperty(keyName, "some value");
            config.setProperty(keyName, "some other value");
            String value = config.getStringProperty(keyName);

            // assert

            // return value is the sysprop
            Assert.assertEquals("some value", value);

            // the key is present and its value has been changed to that of the sysprop
            Assert.assertTrue(config.containsKey(keyName));
            Assert.assertEquals("some value", config.getRawStringProperty(keyName));

            // the backup key has been added and its value is the previously-set value of the (non-backup) key
            Assert.assertTrue(config.containsKey(keyName2));
            Assert.assertEquals("some other value", config.getRawStringProperty(keyName2));

        } finally {
            config.clearProperty(keyName);
            config.clearProperty(keyName2);
            System.clearProperty(keyName);
        }
    }

    @Test
    public void testAfterAnOriginalIsCreatedSystemPropertyNoLongerOverrides() {

        // arrange
        final String keyName = "some-key-name";
        final String keyName2 = "original." + keyName;
        Configuration config = Configuration.getInstance();

        try {
            // precondition
            Assert.assertFalse(config.containsKey(keyName));
            Assert.assertFalse(config.containsKey(keyName2));

            // act
            System.setProperty(keyName, "some value");              // A
            config.setProperty(keyName, "some other value");        // B
            String value = config.getStringProperty(keyName);

            // assert
            Assert.assertEquals("some value", value); // equal to the sysprop A
            Assert.assertTrue(config.containsKey(keyName));
            Assert.assertEquals("some value", config.getRawStringProperty(keyName));
            Assert.assertTrue(config.containsKey(keyName2));
            Assert.assertEquals("some other value", config.getRawStringProperty(keyName2));

            // act
            System.setProperty(keyName, "another value");           // C
            config.setProperty(keyName, "another another value");   // D
            String value2 = config.getStringProperty(keyName);

            // assert
            Assert.assertEquals("another another value", value2); // _not_ equal to the sysprop C
            Assert.assertTrue(config.containsKey(keyName));
            Assert.assertEquals("another another value", config.getRawStringProperty(keyName));
            Assert.assertTrue(config.containsKey(keyName2));
            Assert.assertEquals("some other value", config.getRawStringProperty(keyName2));

        } finally {
            config.clearProperty(keyName);
            config.clearProperty(keyName2);
            System.clearProperty(keyName);
        }
    }

    @Test
    public void testAfterAnOriginalIsCreatedItIsNeverUpdated() {

        // arrange
        final String keyName = "some-key-name";
        final String keyName2 = "original." + keyName;
        Configuration config = Configuration.getInstance();

        try {
            // precondition
            Assert.assertFalse(config.containsKey(keyName));
            Assert.assertFalse(config.containsKey(keyName2));

            // act
            System.setProperty(keyName, "some value");              // A
            config.setProperty(keyName, "some other value");        // B
            String value = config.getStringProperty(keyName);

            // assert
            Assert.assertEquals("some value", value);
            Assert.assertTrue(config.containsKey(keyName));
            Assert.assertEquals("some value", config.getRawStringProperty(keyName));
            Assert.assertTrue(config.containsKey(keyName2));
            Assert.assertEquals("some other value", config.getRawStringProperty(keyName2)); // equal to config value B

            // act
            System.setProperty(keyName, "another value");           // C
            config.setProperty(keyName, "another another value");   // D
            String value2 = config.getStringProperty(keyName);

            // assert
            Assert.assertEquals("another another value", value2);
            Assert.assertTrue(config.containsKey(keyName));
            Assert.assertEquals("another another value", config.getRawStringProperty(keyName));
            Assert.assertTrue(config.containsKey(keyName2));
            Assert.assertEquals("some other value", config.getRawStringProperty(keyName2)); // _still_ equal to config value B

        } finally {
            config.clearProperty(keyName);
            config.clearProperty(keyName2);
            System.clearProperty(keyName);
        }
    }

    @Test
    public void testWhenSyspropDoesntExistAndTheKeyIsntPresentInConfigAndTheBackupKeyIsPresentThenReturnValueShouldBeNullAndBackupRemainsAsIs() {

        // arrange
        final String keyName = "some-key-name";
        final String keyName2 = "original." + keyName;
        Configuration config = Configuration.getInstance();

        try {
            // precondition
            Assert.assertFalse(config.containsKey(keyName));
            Assert.assertFalse(config.containsKey(keyName2));

            // act
            config.setProperty(keyName2, "more value");
            String value = config.getStringProperty(keyName);

            // assert

            // return value is null
            Assert.assertNull(value);

            // the key is not added
            Assert.assertFalse(config.containsKey(keyName));

            // the backup key is still present and has the same value
            Assert.assertTrue(config.containsKey(keyName2));
            Assert.assertEquals("more value", config.getRawStringProperty(keyName2));

        } finally {
            config.clearProperty(keyName);
            config.clearProperty(keyName2);
            System.clearProperty(keyName);
        }
    }

    @Test
    public void testWhenSyspropDoesntExistAndTheKeyIsPresentInConfigAndTheBackupKeyIsPresentThenReturnValueShouldBeKeysValueAndBackupRemainsAsIs() {

        // arrange
        final String keyName = "some-key-name";
        final String keyName2 = "original." + keyName;
        Configuration config = Configuration.getInstance();

        try {
            // precondition
            Assert.assertFalse(config.containsKey(keyName));
            Assert.assertFalse(config.containsKey(keyName2));

            // act
            config.setProperty(keyName, "some value");
            config.setProperty(keyName2, "more value");
            String value = config.getStringProperty(keyName);

            // assert

            // return value is previously-set value
            Assert.assertEquals("some value", value);

            // key is present and its value is equal to what we set it to
            Assert.assertTrue(config.containsKey(keyName));
            Assert.assertEquals("some value", config.getRawStringProperty(keyName));

            // the backup key is still present and has the same value
            Assert.assertTrue(config.containsKey(keyName2));
            Assert.assertEquals("more value", config.getRawStringProperty(keyName2));

        } finally {
            config.clearProperty(keyName);
            config.clearProperty(keyName2);
            System.clearProperty(keyName);
        }
    }

    @Test
    public void testWhenSyspropExistsAndTheKeyIsntPresentInConfigAndTheBackupKeyIsPresentThenReturnValueShouldBeNullAndBackupRemainsAsIs() {

        // arrange
        final String keyName = "some-key-name";
        final String keyName2 = "original." + keyName;
        Configuration config = Configuration.getInstance();

        try {
            // precondition
            Assert.assertFalse(config.containsKey(keyName));
            Assert.assertFalse(config.containsKey(keyName2));

            // act
            System.setProperty(keyName, "some value");
            config.setProperty(keyName2, "more value");
            String value = config.getStringProperty(keyName);

            // assert

            // return value is null (not sysprop as we might expect)
            Assert.assertNull(value);

            // key is not present
            Assert.assertFalse(config.containsKey(keyName));

            // the backup key is still present and has the same value
            Assert.assertTrue(config.containsKey(keyName2));
            Assert.assertEquals("more value", config.getRawStringProperty(keyName2));

        } finally {
            config.clearProperty(keyName);
            config.clearProperty(keyName2);
            System.clearProperty(keyName);
        }
    }

    @Test
    public void testWhenSyspropExistsAndTheKeyIsPresentInConfigAndTheBackupKeyIsPresentThenReturnValueShouldBeKeysValueAndBackupRemainsAsIs() {

        // arrange
        final String keyName = "some-key-name";
        final String keyName2 = "original." + keyName;
        Configuration config = Configuration.getInstance();

        try {
            // precondition
            Assert.assertFalse(config.containsKey(keyName));
            Assert.assertFalse(config.containsKey(keyName2));

            // act
            System.setProperty(keyName, "some value");
            config.setProperty(keyName, "some other value");
            config.setProperty(keyName2, "more value");
            String value = config.getStringProperty(keyName);

            // assert

            // return value is null (not sysprop as we might expect)
            Assert.assertEquals("some other value", value);

            // key is present and its value is equal to what we set it to
            Assert.assertTrue(config.containsKey(keyName));
            Assert.assertEquals("some other value", config.getRawStringProperty(keyName));

            // the backup key is still present and has the same value
            Assert.assertTrue(config.containsKey(keyName2));
            Assert.assertEquals("more value", config.getRawStringProperty(keyName2));

        } finally {
            config.clearProperty(keyName);
            config.clearProperty(keyName2);
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
