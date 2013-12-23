package com.rackspacecloud.blueflood.types;

import junit.framework.Assert;
import org.junit.Test;

public class StatsTypeTest {
    
    @Test
    public void testFromString() {
        // verify both equalities.
        Assert.assertTrue(RollupType.STATSD_COUNTER == RollupType.fromString("STATSD_COUNTER"));
        Assert.assertTrue(RollupType.STATSD_COUNTER.equals(RollupType.fromString("STATSD_COUNTER")));
        
        Assert.assertTrue(RollupType.STATSD_TIMER == RollupType.fromString("STATSD_TIMER"));
        Assert.assertTrue(RollupType.STATSD_TIMER.equals(RollupType.fromString("STATSD_TIMER")));
        
        Assert.assertTrue(RollupType.STATSD_SET == RollupType.fromString("STATSD_SET"));
        Assert.assertTrue(RollupType.STATSD_SET.equals(RollupType.fromString("STATSD_SET")));
        
        Assert.assertTrue(RollupType.STATSD_GAUGE == RollupType.fromString("STATSD_GAUGE"));
        Assert.assertTrue(RollupType.STATSD_GAUGE.equals(RollupType.fromString("STATSD_GAUGE")));
        
        Assert.assertTrue(RollupType.BF_BASIC == RollupType.fromString("BF_BASIC"));
        Assert.assertTrue(RollupType.BF_BASIC.equals(RollupType.fromString("BF_BASIC")));
    }
    
    @Test
    public void testOthersReturnUnknown() {
        Assert.assertTrue(RollupType.BF_BASIC == RollupType.fromString("SomeInvalidStatsType"));
        Assert.assertTrue(RollupType.BF_BASIC.equals(RollupType.fromString("SomeInvalidStatsType")));
    }
    
    @Test
    public void testNullReturnUnknown() {
        Assert.assertTrue(RollupType.BF_BASIC == RollupType.fromString(null));
        Assert.assertTrue(RollupType.BF_BASIC.equals(RollupType.fromString(null)));
    }
    
    @Test
    public void testToStringRoundTrip() {
        Assert.assertTrue(RollupType.values().length > 0);
        
        for (RollupType type : RollupType.values())
            Assert.assertEquals(type, RollupType.fromString(type.toString()));
    }
}
