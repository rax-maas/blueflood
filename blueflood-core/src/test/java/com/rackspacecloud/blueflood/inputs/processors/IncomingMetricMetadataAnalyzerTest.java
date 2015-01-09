package com.rackspacecloud.blueflood.inputs.processors;

import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.exceptions.CacheException;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.service.IncomingMetricMetadataAnalyzer;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.MetricMetadata;
import com.rackspacecloud.blueflood.utils.TimeValue;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class IncomingMetricMetadataAnalyzerTest extends IntegrationTestBase{

    @Test
    public void test_ScanMetrics_DoesNotStoreTypeAndUnit_ForNumericMetrics() throws Exception {
        MetadataCache cache = MetadataCache.getInstance();
        IncomingMetricMetadataAnalyzer analyzer = new IncomingMetricMetadataAnalyzer(cache);

        ArrayList<IMetric> metricsList = new ArrayList<IMetric>();

        Locator locator = Locator.createLocatorFromDbKey("integer_metric");
        metricsList.add(getRandomIntMetric(locator, 1));

        analyzer.scanMetrics(metricsList);
        String type = cache.get(locator, MetricMetadata.TYPE.name().toLowerCase());
        Assert.assertEquals(null, type);

        String unit = cache.get(locator, MetricMetadata.UNIT.name().toLowerCase());
        Assert.assertEquals(null, unit);
    }

    @Test
    public void test_ScanMetricS_StoresTypeAndUnit_ForStringMetrics() throws  Exception {
        MetadataCache cache = MetadataCache.getInstance();
        IncomingMetricMetadataAnalyzer analyzer = new IncomingMetricMetadataAnalyzer(cache);

        ArrayList<IMetric> metricsList = new ArrayList<IMetric>();

        Locator locator = Locator.createLocatorFromDbKey("string_metric");
        metricsList.add(getRandomStringmetric(locator, 1));

        analyzer.scanMetrics(metricsList);
        String type = cache.get(locator, MetricMetadata.TYPE.name().toLowerCase());
        Assert.assertEquals("S", type);

        String unit = cache.get(locator, MetricMetadata.UNIT.name().toLowerCase());
        Assert.assertEquals("unknown", unit);
    }
}
