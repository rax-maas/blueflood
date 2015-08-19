package com.rackspacecloud.blueflood.inputs.processors;

import com.rackspacecloud.blueflood.cache.MetadataCache;

import com.rackspacecloud.blueflood.io.MetadataIO;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.service.IncomingMetricMetadataAnalyzer;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.InMemoryMetadataIO;
import com.rackspacecloud.blueflood.utils.TimeValue;
import com.rackspacecloud.blueflood.utils.Util;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class IncomingMetricMetadataAnalyzerTest {
    private final MetadataIO metadataIO;

    public IncomingMetricMetadataAnalyzerTest () {
        this.metadataIO = new InMemoryMetadataIO();
    }

    @Test
    public void test_ScanMetrics_DoesNotStoreType_ButStoresUnit_ForNumericMetrics() throws Exception {
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
        Assert.assertEquals("somethings", unit);
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
        metricsList.add(new PreaggregatedMetric(12300000, locator, new TimeValue(1, TimeUnit.DAYS), new BluefloodCounterRollup()));

        analyzer.scanMetrics(metricsList);
        String type = cache.get(locator, MetricMetadata.TYPE.name().toLowerCase());
        Assert.assertEquals(null, type);

        String unit = cache.get(locator, MetricMetadata.UNIT.name().toLowerCase());
        Assert.assertEquals(null, unit);
    }

    @Test
    public void test_ScanMetrics_DoesNotStoreUnit_InCache_IfConfigToEsSet() throws Exception {

        MetadataCache cache = MetadataCache.createLoadingCacheInstance(new TimeValue(5, TimeUnit.MINUTES), 1);
        cache.setIO(this.metadataIO);

        IncomingMetricMetadataAnalyzer analyzer = new IncomingMetricMetadataAnalyzer(cache);
        IncomingMetricMetadataAnalyzer.setEsForUnits(true);
        IncomingMetricMetadataAnalyzer.setEsModuleFoundForUnits(true);

        ArrayList<IMetric> metricsList = new ArrayList<IMetric>();

        Locator locator = Locator.createLocatorFromDbKey("integer_metric");

        metricsList.add(new Metric(locator, 123, 14200000, new TimeValue(1, TimeUnit.DAYS), "somethings"));
        analyzer.scanMetrics(metricsList);

        //type should never be stored for numeric metrics
        String type = cache.get(locator, MetricMetadata.TYPE.name().toLowerCase());
        Assert.assertEquals(null, type);

        String unit = cache.get(locator, MetricMetadata.UNIT.name().toLowerCase());
        Assert.assertEquals(null, unit);
    }

    @Test
    public void test_ScanMetrics_StoresUnit_InCache_IfEsModuleNotFound() throws Exception {

        MetadataCache cache = MetadataCache.createLoadingCacheInstance(new TimeValue(5, TimeUnit.MINUTES), 1);
        cache.setIO(this.metadataIO);

        IncomingMetricMetadataAnalyzer analyzer = new IncomingMetricMetadataAnalyzer(cache);
        IncomingMetricMetadataAnalyzer.setEsForUnits(true);
        IncomingMetricMetadataAnalyzer.setEsModuleFoundForUnits(false);

        ArrayList<IMetric> metricsList = new ArrayList<IMetric>();

        Locator locator = Locator.createLocatorFromDbKey("integer_metric");

        metricsList.add(new Metric(locator, 123, 14200000, new TimeValue(1, TimeUnit.DAYS), "somethings"));
        analyzer.scanMetrics(metricsList);

        //type should never be stored for numeric metrics
        String type = cache.get(locator, MetricMetadata.TYPE.name().toLowerCase());
        Assert.assertEquals(null, type);

        String unit = cache.get(locator, MetricMetadata.UNIT.name().toLowerCase());
        Assert.assertEquals("somethings", unit);
    }

    @After
    public void tearDown() {
        IncomingMetricMetadataAnalyzer.setEsForUnits(Configuration.getInstance().getBooleanProperty(CoreConfig.USE_ES_FOR_UNITS));
        IncomingMetricMetadataAnalyzer.setEsModuleFoundForUnits(Configuration.getInstance().getListProperty(CoreConfig.DISCOVERY_MODULES).contains(Util.ElasticIOPath));
    }
}
