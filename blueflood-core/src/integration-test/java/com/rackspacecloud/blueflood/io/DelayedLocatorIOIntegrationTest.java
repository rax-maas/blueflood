package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.io.astyanax.ADelayedLocatorIO;
import com.rackspacecloud.blueflood.io.datastax.DDelayedLocatorIO;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.utils.Util;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class DelayedLocatorIOIntegrationTest extends IntegrationTestBase {

    private final DDelayedLocatorIO dDelayedLocatorIO = new DDelayedLocatorIO();
    private final ADelayedLocatorIO aDelayedLocatorIO = new ADelayedLocatorIO();

    private List<Locator> testLocators;
    private final int TEST_SLOT = 23;
    private final Granularity TEST_GRANULARITY = Granularity.MIN_20;

    @Before
    public void setup() {
        // create test locators
        testLocators = generateTestLocators("100000", 4, "delayed_locator_io.integration.test", 2);
    }

    @Test
    public void writeDatastaxReadAstyanax() throws Exception {

        // insert locators using datastax
        dDelayedLocatorIO.insertLocator(TEST_GRANULARITY, TEST_SLOT, testLocators.get(0));
        dDelayedLocatorIO.insertLocator(TEST_GRANULARITY, TEST_SLOT, testLocators.get(1));

        // retrieve locators using astyanax class
        int shard1 = Util.getShard(testLocators.get(0).toString());
        Collection<Locator> locatorsResult1 = aDelayedLocatorIO.getLocators(SlotKey.of(TEST_GRANULARITY, TEST_SLOT, shard1));
        assertEquals("Unexpected number of locators result for shard1", 1, locatorsResult1.size());
        assertEquals("test locator(0) not equal", testLocators.get(0).toString(), locatorsResult1.toArray()[0].toString());

        int shard2 = Util.getShard(testLocators.get(1).toString());
        Collection<Locator> locatorsResult2 = aDelayedLocatorIO.getLocators(SlotKey.of(TEST_GRANULARITY, TEST_SLOT, shard2));
        assertEquals("Unexpected number of locators result for shard2", 1, locatorsResult2.size());
        assertEquals("test locator(1) not equal", testLocators.get(1).toString(), locatorsResult2.toArray()[0].toString());

        // assert invalid shard should return empty collection using datastax
        assertEquals("locators should be empty", aDelayedLocatorIO.getLocators(SlotKey.of(Granularity.MIN_5, TEST_SLOT, 1)), Collections.emptySet());
    }

    @Test
    public void writeAstyanaxReadDatastax() throws Exception {

        // insert locators using astyanax class
        aDelayedLocatorIO.insertLocator(TEST_GRANULARITY, TEST_SLOT, testLocators.get(2));
        aDelayedLocatorIO.insertLocator(TEST_GRANULARITY, TEST_SLOT, testLocators.get(3));

        // retrieve locators using datastax
        int shard1 = Util.getShard(testLocators.get(2).toString());
        Collection<Locator> locatorsResult1 = dDelayedLocatorIO.getLocators(SlotKey.of(TEST_GRANULARITY, TEST_SLOT, shard1));
        assertEquals("Unexpected number of locators result for shard1", 1, locatorsResult1.size());
        assertEquals("test locator(2) not equal", testLocators.get(2).toString(), locatorsResult1.toArray()[0].toString());

        int shard2 = Util.getShard(testLocators.get(3).toString());
        Collection<Locator> locatorsResult2 = dDelayedLocatorIO.getLocators(SlotKey.of(TEST_GRANULARITY, TEST_SLOT, shard2));
        assertEquals("Unexpected number of locators result for shard2", 1, locatorsResult2.size());
        assertEquals("test locator(3) not equal", testLocators.get(3).toString(), locatorsResult2.toArray()[0].toString());

        // assert invalid shard should return empty collection using datastax
        assertEquals("locators should be empty", dDelayedLocatorIO.getLocators(SlotKey.of(Granularity.MIN_5, TEST_SLOT, 1)), Collections.emptySet());
    }
}
