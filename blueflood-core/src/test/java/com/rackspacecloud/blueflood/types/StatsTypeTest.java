package com.rackspacecloud.blueflood.types;

import junit.framework.Assert;
import org.junit.Test;

public class StatsTypeTest {
    
    @Test
    public void testFromString() {
        // verify both equalities.
        Assert.assertTrue(RollupType.COUNTER == RollupType.fromString("COUNTER"));
        Assert.assertTrue(RollupType.COUNTER.equals(RollupType.fromString("COUNTER")));
        
        Assert.assertTrue(RollupType.TIMER == RollupType.fromString("TIMER"));
        Assert.assertTrue(RollupType.TIMER.equals(RollupType.fromString("TIMER")));
        
        Assert.assertTrue(RollupType.SET == RollupType.fromString("SET"));
        Assert.assertTrue(RollupType.SET.equals(RollupType.fromString("SET")));
        
        Assert.assertTrue(RollupType.GAUGE == RollupType.fromString("GAUGE"));
        Assert.assertTrue(RollupType.GAUGE.equals(RollupType.fromString("GAUGE")));
        
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
