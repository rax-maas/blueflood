package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.io.astyanax.AExcessEnumIO;
import com.rackspacecloud.blueflood.io.datastax.DExcessEnumIO;
import com.rackspacecloud.blueflood.types.Locator;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExcessEnumIOIntegrationTest extends IntegrationTestBase {

    private final DExcessEnumIO datastaxExcessEnumIO = new DExcessEnumIO();
    private final AExcessEnumIO astyanaxExcessEnumIO = new AExcessEnumIO();

    private List<Locator> testLocators;

    @Before
    public void setup() {
        // create test locators
        testLocators = generateTestLocators("100000", 2, "excess_enum_io.integration.test", 2);
    }

    @Test
    public void writeDatastaxReadAstyanax() throws Exception {

        // insert using datastax
        for (Locator testLocator : testLocators) {
            datastaxExcessEnumIO.insertExcessEnumMetric(testLocator);
        }

        Thread.sleep(1000); // wait 1s for inserts

        // retrieve using astyanax class
        Set<Locator> excessEnums = astyanaxExcessEnumIO.getExcessEnumMetrics();

        // assert locators of excess enum metrics
        assertEquals("Unexpected number of excessEnums", testLocators.size(), excessEnums.size());

        for (Locator expectedLocator : testLocators) {
            assertTrue("excess enum does not contain " + expectedLocator, excessEnums.contains(expectedLocator));
        }
    }

    @Test
    public void writeAstyanaxReadDatastax() throws Exception {

        // insert using astyanax
        for (Locator testLocator : testLocators) {
            astyanaxExcessEnumIO.insertExcessEnumMetric(testLocator);
        }

        // retrieve using datastax class
        Set<Locator> excessEnums = datastaxExcessEnumIO.getExcessEnumMetrics();

        // assert locators of excess enum metrics
        assertEquals("Unexpected number of excessEnums", testLocators.size(), excessEnums.size());

        // build locators string delimited by "," and assert locator values
        for (Locator expectedLocator : testLocators) {
            assertTrue("excess enum does not contain " + expectedLocator, excessEnums.contains(expectedLocator));
        }
    }

}