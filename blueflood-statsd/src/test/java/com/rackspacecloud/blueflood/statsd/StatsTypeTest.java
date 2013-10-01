package com.rackspacecloud.blueflood.statsd;

import junit.framework.Assert;
import org.junit.Test;

public class StatsTypeTest {
    
    @Test
    public void testFromString() {
        // verify both equalities.
        Assert.assertTrue(StatType.COUNTER == StatType.fromString("COUNTER"));
        Assert.assertTrue(StatType.COUNTER.equals(StatType.fromString("COUNTER")));
        
        Assert.assertTrue(StatType.TIMER == StatType.fromString("TIMER"));
        Assert.assertTrue(StatType.TIMER.equals(StatType.fromString("TIMER")));
        
        Assert.assertTrue(StatType.SET == StatType.fromString("SET"));
        Assert.assertTrue(StatType.SET.equals(StatType.fromString("SET")));
        
        Assert.assertTrue(StatType.GAUGE == StatType.fromString("GAUGE"));
        Assert.assertTrue(StatType.GAUGE.equals(StatType.fromString("GAUGE")));
        
        Assert.assertTrue(StatType.UNKNOWN == StatType.fromString("UNKNOWN"));
        Assert.assertTrue(StatType.UNKNOWN.equals(StatType.fromString("UNKNOWN")));
    }
    
    @Test
    public void testOthersReturnUnknown() {
        Assert.assertTrue(StatType.UNKNOWN == StatType.fromString("SomeInvalidStatsType"));
        Assert.assertTrue(StatType.UNKNOWN.equals(StatType.fromString("SomeInvalidStatsType")));
    }
    
    @Test
    public void testNullReturnUnknown() {
        Assert.assertTrue(StatType.UNKNOWN == StatType.fromString(null));
        Assert.assertTrue(StatType.UNKNOWN.equals(StatType.fromString(null)));
    }
    
    @Test
    public void testToStringRoundTrip() {
        Assert.assertTrue(StatType.values().length > 0);
        
        for (StatType type : StatType.values())
            Assert.assertEquals(type, StatType.fromString(type.toString()));
    }
}
