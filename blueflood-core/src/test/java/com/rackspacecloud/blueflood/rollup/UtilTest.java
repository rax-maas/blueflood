package com.rackspacecloud.blueflood.rollup;

import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.utils.Util;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class UtilTest {
    private static final Random rand = new Random();
    
    private static String randomString(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++)
            sb = sb.append((char)(rand.nextInt(94)+32));
        return sb.toString();
    }
    
    @Test
    public void testComputeShard() {
        for (int i = 0; i < 10000; i++) {
            int shard = Util.computeShard(randomString(rand.nextInt(100) + 1));
            Assert.assertTrue(shard >= 0);
            Assert.assertTrue(shard < Constants.NUMBER_OF_SHARDS);
        }
    }
    
    @Test
    public void testParseShards() {
        Assert.assertEquals(128, Util.parseShards("ALL").size());
        Assert.assertEquals(0, Util.parseShards("NONE").size());
        Assert.assertEquals(5, Util.parseShards("1,9,4,23,0").size());
        
        try {
            Util.parseShards("1,x,23");
            Assert.assertTrue("Should not have gotten here.", false);
        } catch (NumberFormatException expected) {}
        
        try {
            Util.parseShards("EIGHTY");
            Assert.assertTrue("Should not have gotten here.", false);
        } catch (NumberFormatException expected) {}
        
        try {
            Util.parseShards("1,2,3,4,0,-1");
            Assert.assertTrue("Should not have gotten here.", false);
        } catch (NumberFormatException expected) {}

        boolean exception = false;
        try {
            Util.parseShards("" + (Constants.NUMBER_OF_SHARDS + 1));
        } catch (NumberFormatException expected) {
            exception = true;
            Assert.assertEquals("Invalid shard identifier: 129", expected.getMessage());
        }

        Assert.assertEquals(true, exception);
    }

    @Test
    public void testFormatStateColumnName() {
        Assert.assertEquals("metrics_full,1,okay", Util.formatStateColumnName(Granularity.FULL, 1, "okay"));
    }

    @Test
    public void testGranularityFromStateCol() {
        Granularity myGranularity = Util.granularityFromStateCol("metrics_full,1,okay");
        Assert.assertNotNull(myGranularity);
        Assert.assertEquals(myGranularity, Granularity.FULL);

        myGranularity = Util.granularityFromStateCol("FULL");
        Assert.assertNull(myGranularity);
    }

    @Test
    public void testSlotFromStateCol() {
        Assert.assertEquals(1, Util.slotFromStateCol("metrics_full,1,okay"));
    }

    @Test
    public void testStateFromStateCol() {
        Assert.assertEquals("okay", Util.stateFromStateCol("metrics_full,1,okay"));
    }

    @Test
    public void testIsExternalMetric() {
        Assert.assertEquals(true, Util.isExternalMetric("mzORD.blah"));
        Assert.assertEquals(false, Util.isExternalMetric("dim0.blah"));
    }

    @Test
    public void testGetDimensionFromKey() {
        Assert.assertEquals("mzORD", Util.getDimensionFromKey("mzORD.blah"));
        Assert.assertEquals("dim0", Util.getDimensionFromKey("dim0.blah"));
    }

    @Test
    public void testGetMetricFromKey() {
        Assert.assertEquals("blah.sawtooth", Util.getMetricFromKey("mzGRD.blah.sawtooth"));
        Assert.assertEquals("blah", Util.getMetricFromKey("mzGRD.blah"));
        Assert.assertEquals("sawtooth", Util.getMetricFromKey("dim0.sawtooth"));
    }
}
