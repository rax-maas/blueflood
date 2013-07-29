package com.cloudkick.blueflood.rollup;

import com.cloudkick.blueflood.inputs.formats.CloudMonitoringTelescope;
import com.cloudkick.blueflood.io.Constants;
import com.cloudkick.blueflood.types.Average;
import com.cloudkick.blueflood.utils.MetricHelper;
import com.cloudkick.blueflood.utils.Util;
import org.junit.Assert;
import org.junit.Test;
import telescope.thrift.Metric;

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
    public void testGetMetricValue() {
        Metric m1 = new Metric((byte) MetricHelper.Type.DOUBLE);
        Metric m2 = new Metric((byte)MetricHelper.Type.STRING);
        Metric m3 = new Metric((byte)MetricHelper.Type.INT32);
        Metric m4 = new Metric((byte)MetricHelper.Type.INT64);
        Metric m5 = new Metric((byte)MetricHelper.Type.UINT32);
        Metric m6 = new Metric((byte)'z');
        m1.setValueDbl(100.0);
        m2.setValueStr("a");
        m3.setValueI32(7);
        m4.setValueI64(23);
        m5.setValueI32(1991);

        Assert.assertEquals(100.0, CloudMonitoringTelescope.getMetricValue(m1));
        Assert.assertEquals("a", CloudMonitoringTelescope.getMetricValue(m2));
        Assert.assertEquals(7, CloudMonitoringTelescope.getMetricValue(m3));
        Assert.assertEquals(new Long(23), (Long)(CloudMonitoringTelescope.getMetricValue(m4)));
        Assert.assertEquals((Long)23L, (Long)(CloudMonitoringTelescope.getMetricValue(m4)));

        boolean failed = false;

        try {
            CloudMonitoringTelescope.getMetricValue(m6);
        }
        catch (RuntimeException e) {
            failed = true;
            Assert.assertEquals("Unexpected metric type: " + (char)m6.getMetricType(), e.getMessage());
        }

        Assert.assertEquals(true, failed);
    }

    @Test
    public void testCreateMetric() {
        Double myDouble = new Double(66.6);
        Long myLong = new Long(4578);
        Integer myInteger = new Integer(1224);
        Average myAverage1 = new Average(1, myDouble);
        Average myAverage2 = new Average(1, myLong);
        Metric myMetric = new Metric((byte) MetricHelper.Type.DOUBLE);

        Metric m;

        m = Util.createMetric(myDouble);
        Assert.assertEquals(66.6, CloudMonitoringTelescope.getMetricValue(m));

        m = Util.createMetric(myLong);
        Assert.assertEquals(new Long(4578), (Long) m.getValueI64());
        Assert.assertEquals((Long) 4578L, (Long) m.getValueI64());

        m = Util.createMetric(myInteger);
        Assert.assertEquals(1224, CloudMonitoringTelescope.getMetricValue(m));

        m = Util.createMetric(myAverage1);
        Assert.assertEquals(66.6, CloudMonitoringTelescope.getMetricValue(m));

        m = Util.createMetric(myAverage2);
        Assert.assertEquals(new Long(4578), (Long) CloudMonitoringTelescope.getMetricValue(m));
        Assert.assertEquals((Long)4578L, (Long) CloudMonitoringTelescope.getMetricValue(m));

        boolean failed = false;

        try {
            m = Util.createMetric(myMetric);
        }
        catch (RuntimeException e) {
            failed = true;
            Assert.assertEquals("Unexpected type for rollup: telescope.thrift.Metric", e.getMessage());
        }

        Assert.assertEquals(true, failed);
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
