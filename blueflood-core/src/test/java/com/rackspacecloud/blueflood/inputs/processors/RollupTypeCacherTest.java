/*
 * Copyright 2015 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.inputs.processors;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.types.*;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import static org.mockito.Mockito.*;

public class RollupTypeCacherTest {
    private static final int METRICS_PER_LIST = 4;
    private static final String cacheKey = MetricMetadata.ROLLUP_TYPE.name().toLowerCase();

    private static MetricsCollection createTestData() {
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

    private static MetricsCollection createPreaggregatedTestMetrics() {
        List<IMetric> listMetrics = new ArrayList<IMetric>();
        for (int j=0; j < METRICS_PER_LIST; j++) {
            IMetric m = mock(PreaggregatedMetric.class);
            listMetrics.add(m);

            stub(m.getLocator()).toReturn(Locator.createLocatorFromDbKey("Counter" + j));
            stub(m.getCollectionTime()).toReturn(1234500000+(long)j);
            stub(m.getRollupType()).toReturn(RollupType.COUNTER);
        }
        MetricsCollection collection = new MetricsCollection();
        collection.add(listMetrics);
        return collection;
    }

    @Test
    public void testCacher() throws Exception {
        MetricsCollection collection = createTestData();

        MetadataCache rollupTypeCache = mock(MetadataCache.class);
        ThreadPoolExecutor tpe = 
            new ThreadPoolBuilder().withName("rtc test").build();

        RollupTypeCacher rollupTypeCacher = new RollupTypeCacher(tpe,
            rollupTypeCache);

        rollupTypeCacher.apply(collection);

        // wait till done
        while (tpe.getCompletedTaskCount() < 1) {
            Thread.sleep(1);
        }

        verifyZeroInteractions(rollupTypeCache);
    }

    @Test
    public void testCacherDoesCacheRollupTypeForPreAggregatedMetrics() throws Exception {
        MetricsCollection collection = createPreaggregatedTestMetrics();

        MetadataCache rollupTypeCache = mock(MetadataCache.class);
        ThreadPoolExecutor tpe =
                new ThreadPoolBuilder().withName("rtc test").build();

        RollupTypeCacher rollupTypeCacher = new RollupTypeCacher(tpe,
                rollupTypeCache);

        rollupTypeCacher.apply(collection);

        // wait till done
        while (tpe.getCompletedTaskCount() < 1) {
            Thread.sleep(1);
        }

        // Confirm that each metric is cached
        for (IMetric m : collection.toMetrics()) {
            verify(rollupTypeCache).put(m.getLocator(), cacheKey,
                    m.getRollupType().toString());
        }
    }
}
