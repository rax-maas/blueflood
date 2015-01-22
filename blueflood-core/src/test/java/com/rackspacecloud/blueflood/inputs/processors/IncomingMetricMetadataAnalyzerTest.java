package com.rackspacecloud.blueflood.inputs.processors;

import com.rackspacecloud.blueflood.cache.MetadataCache;

import com.rackspacecloud.blueflood.io.MetadataIO;
import com.rackspacecloud.blueflood.service.IncomingMetricMetadataAnalyzer;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.InMemoryMetadataIO;
import com.rackspacecloud.blueflood.utils.TimeValue;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class IncomingMetricMetadataAnalyzerTest {
    private final MetadataIO metadataIO;

    public IncomingMetricMetadataAnalyzerTest () {
        this.metadataIO = new InMemoryMetadataIO();
    }

    @Test
    public void test_ScanMetrics_DoesNotStoreTypeAndUnit_ForNumericMetrics() throws Exception {
        MetadataCache cache = MetadataCache.createLoadingCacheInstance(new TimeValue(5, TimeUnit.MINUTES), 1);
        cache.setIO(this.metadataIO);
        IncomingMetricMetadataAnalyzer analyzer = new IncomingMetricMetadataAnalyzer(cache);

        ArrayList<IMetric> metricsList = new ArrayList<IMetric>();

        Locator locator = Locator.createLocatorFromDbKey("integer_metric");
        metricsList.add(new Metric(locator, 123, 14200000, new TimeValue(1, TimeUnit.DAYS),"somethings"));

        analyzer.scanMetrics(metricsList);
        String type = cache.get(locator, MetricMetadata.TYPE.name().toLowerCase());
        Assert.assertEquals(null, type);

        String unit = cache.get(locator, MetricMetadata.UNIT.name().toLowerCase());
        Assert.assertEquals(null, unit);
    }

    @Test
    public void test_ScanMetricS_StoresTypeAndUnit_ForStringMetrics() throws  Exception {
        MetadataCache cache = MetadataCache.createLoadingCacheInstance(new TimeValue(5, TimeUnit.MINUTES), 1);
        cache.setIO(this.metadataIO);
        IncomingMetricMetadataAnalyzer analyzer = new IncomingMetricMetadataAnalyzer(cache);

        ArrayList<IMetric> metricsList = new ArrayList<IMetric>();

        Locator locator = Locator.createLocatorFromDbKey("string_metric");
        metricsList.add(new Metric(locator, "verystringy", 142000001, new TimeValue(1, TimeUnit.DAYS),"somethings"));

        analyzer.scanMetrics(metricsList);
        String type = cache.get(locator, MetricMetadata.TYPE.name().toLowerCase());
        Assert.assertEquals("S", type);

        String unit = cache.get(locator, MetricMetadata.UNIT.name().toLowerCase());
        Assert.assertEquals("somethings", unit);
    }

    @Test
    public void test_ScanMetricS_DoesNotStoreTypeAndUnit_ForPreaggregatedMetrics() throws  Exception {
        MetadataCache cache = MetadataCache.createLoadingCacheInstance(new TimeValue(5, TimeUnit.MINUTES), 1);
        cache.setIO(this.metadataIO);
        IncomingMetricMetadataAnalyzer analyzer = new IncomingMetricMetadataAnalyzer(cache);

        ArrayList<IMetric> metricsList = new ArrayList<IMetric>();

        Locator locator = Locator.createLocatorFromDbKey("preag_counter");
        metricsList.add(new PreaggregatedMetric(12300000, locator, new TimeValue(1, TimeUnit.DAYS), new CounterRollup()));

        analyzer.scanMetrics(metricsList);
        String type = cache.get(locator, MetricMetadata.TYPE.name().toLowerCase());
        Assert.assertEquals(null, type);

        String unit = cache.get(locator, MetricMetadata.UNIT.name().toLowerCase());
        Assert.assertEquals(null, unit);
    }
}
