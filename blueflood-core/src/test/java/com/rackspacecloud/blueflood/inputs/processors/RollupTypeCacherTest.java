package com.rackspacecloud.blueflood.inputs.processors;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.MetricsCollection;
import com.rackspacecloud.blueflood.types.MetricMetadata;
import com.rackspacecloud.blueflood.types.RollupType;
import com.rackspacecloud.blueflood.utils.TimeValue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;

public class RollupTypeCacherTest {
    private static final int METRICS_PER_LIST = 4;
    private static final String cacheKey = MetricMetadata.ROLLUP_TYPE.name().toLowerCase();

    static MetricsCollection createTestData() {
        // create fake metrics to test
        Integer counter = 0;
        List<IMetric> l = new ArrayList<IMetric>();
        for (int j = 0; j < METRICS_PER_LIST; j++) {
            counter++;
            IMetric m = mock(Metric.class);
            l.add(m);
            // setup the metric stubs
            stub(m.getLocator()).
                toReturn(Locator.createLocatorFromDbKey(counter.toString()));
            stub(m.getCollectionTime()).toReturn(counter.longValue());
            stub(m.getRollupType()).toReturn(RollupType.BF_BASIC);
        }
        
        MetricsCollection collection = new MetricsCollection();
        collection.add(new ArrayList<IMetric>(l));
        return collection;
    }

    @Test
    public void testCacher() throws Exception {
        MetricsCollection collection = createTestData();

        MetadataCache rollupTypeCache = MetadataCache.createLoadingCacheInstance(
            new TimeValue(1, TimeUnit.HOURS), 10);       
        RollupTypeCacher rollupTypeCacher = new RollupTypeCacher(
            new ThreadPoolBuilder().withName("rtc test").build(),
            rollupTypeCache);

        rollupTypeCacher.apply(collection);

        // Confirm that each metric is cached
        for (IMetric m : collection.toMetrics()) {
            Assert.assertEquals(rollupTypeCache.get(m.getLocator(), cacheKey), 
                m.getRollupType().toString());
        }
    }
}
