package com.rackspacecloud.blueflood.types;

import com.rackspacecloud.blueflood.CloudMonitoringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;

public class ServerMetricLocatorTest {
    private ServerMetricLocator locator;

    @Before
    public void setUp() {
        // left empty
    }

    @Test
    public void testLocatorConstructionHappyCase() {
        locator = ServerMetricLocator.createFromTelescopePrimitives("someAccount", "someEntity", "someCheck",
                "someDim.someMetric");
        Assert.assertEquals("someAccount,someEntity,someCheck,someDim.someMetric", locator.toString());
        
        locator = ServerMetricLocator.createFromTelescopePrimitives("someAccount", "someEntity", "someCheck",
                CloudMonitoringUtils.generateMetricName("someMetric", "mzSomeMonitoringZone"));
        Assert.assertEquals("someAccount,someEntity,someCheck,mzSomeMonitoringZone.someMetric", locator.toString());
    }

    @Test
    public void testLocatorConstructionWithCommaInArgs() {
        locator = ServerMetricLocator.createFromTelescopePrimitives("someAccount", "someEntity",
                "someCheck,testingComma", "someDim.someMetric");
        Assert.assertEquals("someAccount,someEntity,someCheck_testingComma,someDim.someMetric", locator.toString());
        
        locator = ServerMetricLocator.createFromTelescopePrimitives("someAccount", "someEntity", "someCheck,testingComma",
                CloudMonitoringUtils.generateMetricName("someMetric", "mzSomeMonitoringZone"));
        Assert.assertEquals("someAccount,someEntity,someCheck_testingComma,mzSomeMonitoringZone.someMetric", locator.toString());
    }

    @Test
    public void testLocatorConstructionFromString() {
        locator = ServerMetricLocator.createFromDBKey("someAccount,someEntity,someCheck_testingComma,someDim.someMetric");
        Assert.assertEquals("someCheck_testingComma", locator.getCheckId());
        Assert.assertEquals("someDim.someMetric", locator.getMetric());
        Assert.assertEquals("someAccount", locator.getAccountId());
        Assert.assertEquals("someEntity", locator.getEntityId());
        Assert.assertEquals("someEntity,someCheck_testingComma,someDim.someMetric", locator.getMetricName());
    }

    @Test
    public void testLocatorConstructionFromBadString() {
        try {
            locator = ServerMetricLocator.createFromDBKey("someCheck_someMetric_someDim");
            fail("Locator construction should have failed");
        } catch (Exception e) {
            // test pass
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
    }
}
