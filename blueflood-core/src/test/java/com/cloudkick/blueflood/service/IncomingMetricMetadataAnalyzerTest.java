package com.cloudkick.blueflood.service;

import com.cloudkick.blueflood.cache.MetadataCache;
import com.cloudkick.blueflood.inputs.formats.CloudMonitoringTelescope;
import com.cloudkick.blueflood.utils.MetricHelper;
import com.cloudkick.blueflood.utils.TimeValue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;
import telescope.thrift.Metric;
import telescope.thrift.Telescope;
import telescope.thrift.VerificationModel;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class IncomingMetricMetadataAnalyzerTest {
    private IncomingMetricMetadataAnalyzer metadataAnalyzer;
    private CloudMonitoringTelescope cmTelescope;
    private Metric int1;
    private Metric long1;
    private Metric long2;
    private Metric long3;
    private Metric double1;
    private Metric string1;

    @Before
    public void setUp() {
        TimeValue timeValue = new TimeValue(10, TimeUnit.SECONDS);
        int concurrency = 1;
        final MetadataCache nonDatabaseCache = MetadataCache.createInMemoryCacheInstance(timeValue, concurrency);
        metadataAnalyzer = new IncomingMetricMetadataAnalyzer(nonDatabaseCache);

        Whitebox.setInternalState(metadataAnalyzer, "cache", nonDatabaseCache);

        int1 = new Metric((byte) MetricHelper.Type.INT32);
        long1 = new Metric((byte)MetricHelper.Type.UINT32);
        long2 = new Metric((byte)MetricHelper.Type.INT64);
        long3 = new Metric((byte)MetricHelper.Type.UINT64);
        double1 = new Metric((byte)MetricHelper.Type.DOUBLE);
        string1 = new Metric((byte)MetricHelper.Type.STRING);

        int1.setValueI32(1);
        long1.setValueI64(1L);
        long2.setValueI64(1L);
        long3.setValueI64(1L);
        double1.setValueDbl(1d);
        string1.setValueStr("1");

        Map<String, Metric> metrics = new HashMap<String, Metric>();
        metrics.put("int1", int1);
        metrics.put("long1", long1);
        metrics.put("long2", long2);
        metrics.put("long3", long3);
        metrics.put("double1", double1);
        metrics.put("string1", string1);

        Telescope tel = new Telescope("id", "checkId", "acctId", "checkModule", "entityId", "target", 1000L, 1, VerificationModel.ONE);
        tel.setMonitoringZoneId("mzGRD");
        tel.setMetrics(metrics);
        cmTelescope = new CloudMonitoringTelescope(tel);
    }

    @Test
    public void testInitialAddDetectsNoChanges() {
        // adding all when null should generate no exceptions.
        Assert.assertEquals(0, metadataAnalyzer.scanMetrics(cmTelescope.toMetrics()).size());
    }

    @Test
    public void testIndividualChanges() {
        // initialize.
        metadataAnalyzer.scanMetrics(cmTelescope.toMetrics());

        // verify that each metric is caught as it goes through.
        int1.setMetricType((byte)MetricHelper.Type.UINT32);
        Assert.assertEquals(1, metadataAnalyzer.scanMetrics(cmTelescope.toMetrics()).size());

        long1.setMetricType((byte)MetricHelper.Type.INT32);
        Assert.assertEquals(1, metadataAnalyzer.scanMetrics(cmTelescope.toMetrics()).size());

        long2.setMetricType((byte) MetricHelper.Type.INT32);
        Assert.assertEquals(1, metadataAnalyzer.scanMetrics(cmTelescope.toMetrics()).size());

        long3.setMetricType((byte)MetricHelper.Type.INT32);
        Assert.assertEquals(1, metadataAnalyzer.scanMetrics(cmTelescope.toMetrics()).size());

        // for fun, do a double change and verify that it is caught.
        double1.setMetricType((byte)MetricHelper.Type.INT32);
        string1.setMetricType((byte) MetricHelper.Type.INT32);
        Assert.assertEquals(2, metadataAnalyzer.scanMetrics(cmTelescope.toMetrics()).size());
    }

    @Test
    public void testChangeBackCaught() {
        metadataAnalyzer.scanMetrics(cmTelescope.toMetrics());
        byte originalType = int1.getMetricType();

        // type toggles to long.
        int1.setMetricType((byte)MetricHelper.Type.UINT32);
        Assert.assertEquals(1, metadataAnalyzer.scanMetrics(cmTelescope.toMetrics()).size());

        // subsequent scans detect no chagnes.
        Assert.assertEquals(0, metadataAnalyzer.scanMetrics(cmTelescope.toMetrics()).size());
        Assert.assertEquals(0, metadataAnalyzer.scanMetrics(cmTelescope.toMetrics()).size());

        // type goes back to original. change should be detected.
        int1.setMetricType(originalType);
        Assert.assertEquals(1, metadataAnalyzer.scanMetrics(cmTelescope.toMetrics()).size());
    }
}
