package com.rackspacecloud.blueflood.thrift;

import com.rackspacecloud.blueflood.inputs.formats.CloudMonitoringTelescope;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.types.Average;
import com.rackspacecloud.blueflood.utils.MetricHelper;
import com.rackspacecloud.blueflood.utils.Util;
import junit.framework.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;
import telescope.thrift.Metric;
import telescope.thrift.UnitEnum;

// todo: CM_SPECIFIC
public class ThriftAssumptionTest {
    
    // if this test ever breaks, we know that something change in blueflood upstream.
    @Test
    public void testUnknownAsString() {
        String astyanaxReaderConstant = (String)Whitebox.getInternalState(AstyanaxReader.getInstance(), "UNKNOWN_UNIT");
        Assert.assertEquals(UnitEnum.UNKNOWN.toString().toLowerCase(), astyanaxReaderConstant);
    }
    
    // these next few tests were moved from c.r.b.service.UtilTest
    
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
    
            org.junit.Assert.assertEquals(100.0, CloudMonitoringTelescope.getMetricValue(m1));
            org.junit.Assert.assertEquals("a", CloudMonitoringTelescope.getMetricValue(m2));
            org.junit.Assert.assertEquals(7, CloudMonitoringTelescope.getMetricValue(m3));
            org.junit.Assert.assertEquals(new Long(23), (Long) (CloudMonitoringTelescope.getMetricValue(m4)));
            org.junit.Assert.assertEquals((Long) 23L, (Long) (CloudMonitoringTelescope.getMetricValue(m4)));
    
            boolean failed = false;
    
            try {
                CloudMonitoringTelescope.getMetricValue(m6);
            }
            catch (RuntimeException e) {
                failed = true;
                org.junit.Assert.assertEquals("Unexpected metric type: " + (char) m6.getMetricType(), e.getMessage());
            }
    
            org.junit.Assert.assertEquals(true, failed);
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
        org.junit.Assert.assertEquals(66.6, CloudMonitoringTelescope.getMetricValue(m));

        m = Util.createMetric(myLong);
        org.junit.Assert.assertEquals(new Long(4578), (Long) m.getValueI64());
        org.junit.Assert.assertEquals((Long) 4578L, (Long) m.getValueI64());

        m = Util.createMetric(myInteger);
        org.junit.Assert.assertEquals(1224, CloudMonitoringTelescope.getMetricValue(m));

        m = Util.createMetric(myAverage1);
        org.junit.Assert.assertEquals(66.6, CloudMonitoringTelescope.getMetricValue(m));

        m = Util.createMetric(myAverage2);
        org.junit.Assert.assertEquals(new Long(4578), (Long) CloudMonitoringTelescope.getMetricValue(m));
        org.junit.Assert.assertEquals((Long) 4578L, (Long) CloudMonitoringTelescope.getMetricValue(m));

        boolean failed = false;

        try {
            m = Util.createMetric(myMetric);
        }
        catch (RuntimeException e) {
            failed = true;
            org.junit.Assert.assertEquals("Unexpected type for rollup: telescope.thrift.Metric", e.getMessage());
        }

        org.junit.Assert.assertEquals(true, failed);
    }
}
