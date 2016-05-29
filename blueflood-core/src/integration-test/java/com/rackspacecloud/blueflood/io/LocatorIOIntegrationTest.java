package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.io.astyanax.AstyanaxLocatorIO;
import com.rackspacecloud.blueflood.io.datastax.DatastaxLocatorIO;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.utils.Util;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LocatorIOIntegrationTest extends IntegrationTestBase {

    private final DatastaxLocatorIO datastaxLocatorIO = new DatastaxLocatorIO();
    private final AstyanaxLocatorIO astyanaxLocatorIO = new AstyanaxLocatorIO();

    private List<Locator> testLocators;

    @Before
    public void setup() {
        // create test locators
        testLocators = generateTestLocators("100000", 4, "locator_io.integration.test", 2);
    }

    @Test
    public void writeDatastaxReadAstyanax() throws Exception {

        // insert locators using datastax
        datastaxLocatorIO.insertLocator(testLocators.get(0));
        datastaxLocatorIO.insertLocator(testLocators.get(1));

        // retrieve locators using astyanax class
        long shard1 = (long) Util.getShard(testLocators.get(0).toString());
        Collection<Locator> locatorsResult1 = astyanaxLocatorIO.getLocators(shard1);
        assertEquals("Unexpected number of locators result for shard1", 1, locatorsResult1.size());
        assertEquals("test locator(0) not equal", testLocators.get(0).toString(), locatorsResult1.toArray()[0].toString());
        assertTrue("last update stamp should have value", new ArrayList<Locator>(locatorsResult1).get(0).getLastUpdatedTimestamp() > 0);

        long shard2 = (long) Util.getShard(testLocators.get(1).toString());
        Collection<Locator> locatorsResult2 = astyanaxLocatorIO.getLocators(shard2);
        assertEquals("Unexpected number of locators result for shard2", 1, locatorsResult2.size());
        assertEquals("test locator(1) not equal", testLocators.get(1).toString(), locatorsResult2.toArray()[0].toString());
        assertTrue("last update stamp should have value", new ArrayList<Locator>(locatorsResult2).get(0).getLastUpdatedTimestamp() > 0);

        // assert invalid shard should return empty collection using datastax
        assertEquals("locators should be empty", astyanaxLocatorIO.getLocators(-1), Collections.emptySet());
    }

    @Test
    public void writeAstyanaxReadDatastax() throws Exception {

        // insert locators using astyanax class
        astyanaxLocatorIO.insertLocator(testLocators.get(2));
        astyanaxLocatorIO.insertLocator(testLocators.get(3));

        // retrieve locators using datastax
        long shard1 = (long) Util.getShard(testLocators.get(2).toString());
        Collection<Locator> locatorsResult1 = datastaxLocatorIO.getLocators(shard1);
        assertEquals("Unexpected number of locators result for shard1", 1, locatorsResult1.size());
        assertEquals("test locator(2) not equal", testLocators.get(2).toString(), locatorsResult1.toArray()[0].toString());
        assertTrue("last update stamp should have value", new ArrayList<Locator>(locatorsResult1).get(0).getLastUpdatedTimestamp() > 0);

        long shard2 = (long) Util.getShard(testLocators.get(3).toString());
        Collection<Locator> locatorsResult2 = datastaxLocatorIO.getLocators(shard2);
        assertEquals("Unexpected number of locators result for shard2", 1, locatorsResult2.size());
        assertEquals("test locator(3) not equal", testLocators.get(3).toString(), locatorsResult2.toArray()[0].toString());
        assertTrue("last update stamp should have value", new ArrayList<Locator>(locatorsResult2).get(0).getLastUpdatedTimestamp() > 0);

        // assert invalid shard should return empty collection using datastax
        assertEquals("locators should be empty", datastaxLocatorIO.getLocators(-1), Collections.emptySet());
    }

}
