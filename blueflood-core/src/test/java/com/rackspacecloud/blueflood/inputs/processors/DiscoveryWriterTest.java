/*
 * Copyright 2013-2015 Rackspace
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

import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.cache.LocatorCache;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import static org.mockito.Mockito.*;


public class DiscoveryWriterTest {

    static List<List<IMetric>> createTestData() {
        List<IMetric> l1 = new ArrayList<IMetric>();
        l1.add(mock(IMetric.class));
        l1.add(mock(IMetric.class));
        l1.add(mock(IMetric.class));
        l1.add(mock(IMetric.class));

        List<IMetric> l2 = new ArrayList<IMetric>();
        l2.add(mock(IMetric.class));
        l2.add(mock(IMetric.class));
        l2.add(mock(IMetric.class));
        l2.add(mock(IMetric.class));

        List<IMetric> l3 = new ArrayList<IMetric>();
        l3.add(mock(IMetric.class));
        l3.add(mock(IMetric.class));
        l3.add(mock(IMetric.class));
        l3.add(mock(IMetric.class));

        List<IMetric> l4 = new ArrayList<IMetric>();
        l4.add(mock(IMetric.class));
        l4.add(mock(IMetric.class));
        l4.add(mock(IMetric.class));
        l4.add(mock(IMetric.class));


        // create fake metrics to test
        List<List<IMetric>> testdata = new ArrayList<List<IMetric>>();
        testdata.add(l1);
        testdata.add(l2);
        testdata.add(l3);
        testdata.add(l4);

        //setup locators
        Integer counter = 0;
        for (List<IMetric> l : testdata) {
            for (IMetric m : l) {
                counter++;
                stub(m.getLocator()).toReturn(Locator.createLocatorFromDbKey(counter.toString()));
            }
        }
        
        return testdata;
    }

    List<IMetric> flattenDataset(List<List<IMetric>> dataset) {
        List<IMetric> flatTestData = new ArrayList<IMetric>();
        for (List<IMetric> list : dataset) {
            if (list.size() == 0) continue;
            for (IMetric m : list) {
                flatTestData.add(m);
            }
        }

        return flatTestData;
    }

    @Before
    public void setUp() throws Exception {
        LocatorCache.getInstance().resetCache();
    }

    @Test
    public void testProcessor() throws Exception {
        DiscoveryWriter discWriter =
                new DiscoveryWriter(new ThreadPoolBuilder()
                        .withName("Metric Discovery Writing")
                        .withCorePoolSize(10)
                        .withMaxPoolSize(10)
                        .withUnboundedQueue()
                        .withRejectedHandler(new ThreadPoolExecutor.AbortPolicy())
                        .build());

        DiscoveryIO discovererA = mock(DiscoveryIO.class);
        DiscoveryIO discovererB = mock(DiscoveryIO.class);
        discWriter.registerIO(discovererA);
        discWriter.registerIO(discovererB);

        List<List<IMetric>> testdata = createTestData();

        List<IMetric> flatTestData = new ArrayList<IMetric>();
        for (List<IMetric> list : testdata) {
            if (list.size() == 0) continue;
            for (IMetric m : list) {
                flatTestData.add(m);
            }
        }

        // Make sure we have data
        Assert.assertTrue(flatTestData.size() > 0);

        ListenableFuture<Boolean> result = discWriter.processMetrics(testdata);
        // wait until DiscoveryWriter finishes processing all the fake metrics
        Assert.assertTrue(result.get());

        // verify the insertDiscovery method on all implementors of
        // DiscoveryIO has been called on all metrics
        verify(discovererA).insertDiscovery(flatTestData);
        verify(discovererB).insertDiscovery(flatTestData);
    }

    /**
     * After metrics are processed, they should be marked as current in the locator cache.
     * This test verifies whether metrics are correctly marked as current when inserted and that only non-current
     * metrics are inserted.
     */
    @Test
    public void testProcessorMarksIndexedMetricsAsCurrent() throws Exception {
        DiscoveryWriter discWriter =
                new DiscoveryWriter(new ThreadPoolBuilder()
                        .withName("Metric Discovery Writing")
                        .withCorePoolSize(10)
                        .withMaxPoolSize(10)
                        .withUnboundedQueue()
                        .withRejectedHandler(new ThreadPoolExecutor.AbortPolicy())
                        .build());

        DiscoveryIO discovererA = mock(DiscoveryIO.class);
        DiscoveryIO discovererB = mock(DiscoveryIO.class);
        discWriter.registerIO(discovererA);
        discWriter.registerIO(discovererB);

        List<List<IMetric>> testdata = createTestData();

        // Make sure we have data
        Assert.assertTrue(testdata.size() >= 4);

        //Split the dataset in two parts, of which the second also contains some items of the first, so we can test the "mark index as current" logic
        List<List<IMetric>> testdataPart1 = testdata.subList(0, 3);
        List<List<IMetric>> testdataPart2 = testdata.subList(2, 4);
        List<List<IMetric>> testDataPart2MinusPart1Data = testdata.subList(3, 4);
        List<IMetric> flatTestDataPart1 = flattenDataset(testdataPart1);
        List<IMetric> flatTestDataPart2MinusPart1Data = flattenDataset(testDataPart2MinusPart1Data);


        ListenableFuture<Boolean> resultPart1 = discWriter.processMetrics(testdataPart1);
        // wait until DiscoveryWriter finishes processing the first batch of fake metrics
        Assert.assertTrue(resultPart1.get());

        ListenableFuture<Boolean> resultPart2 = discWriter.processMetrics(testdataPart2);
        // wait until DiscoveryWriter finishes processing the first batch of fake metrics
        Assert.assertTrue(resultPart2.get());

        // verify that all batch 1 metrics are inserted on all implementers of DiscoveryIO
        verify(discovererA).insertDiscovery(flatTestDataPart1);
        verify(discovererB).insertDiscovery(flatTestDataPart1);

        // verify that only the part 2 metrics have been inserted which haven't been inserted by part 1
        verify(discovererA).insertDiscovery(flatTestDataPart2MinusPart1Data);
        verify(discovererB).insertDiscovery(flatTestDataPart2MinusPart1Data);
    }

    /**
     * It can occur that a metric is current in the batch layer, but not in the discovery layer. In that case the
     * DiscoveryWriter should still write the metric to the discovery layer. This test verifies this behaviour.
     */
    @Test
    public void testProcessorWritesMetricsToDiscoveryLayerWhenOnlyBatchLayerIsCurrent() throws Exception {
        DiscoveryWriter discWriter =
                new DiscoveryWriter(new ThreadPoolBuilder()
                        .withName("Metric Discovery Writing")
                        .withCorePoolSize(10)
                        .withMaxPoolSize(10)
                        .withUnboundedQueue()
                        .withRejectedHandler(new ThreadPoolExecutor.AbortPolicy())
                        .build());

        DiscoveryIO discovererA = mock(DiscoveryIO.class);
        discWriter.registerIO(discovererA);

        List<List<IMetric>> testdata = createTestData();

        // Make sure we have data
        Assert.assertTrue(testdata.size() >= 2);

        List<IMetric> flatTestData = flattenDataset(testdata);

        // Mark the first metric in the dataset as current in the batch layer, so that we can test whether it gets written to the discovery layer
        LocatorCache.getInstance().setLocatorCurrentInBatchLayer(flatTestData.get(0).getLocator());

        // Then, provide all the metrics in the dataset to the discovery writer
        ListenableFuture<Boolean> result = discWriter.processMetrics(testdata);
        // wait until DiscoveryWriter finishes processing the batch of fake metrics
        Assert.assertTrue(result.get());

        // verify that all metrics are inserted in the discovery layer, even while the first one was current in the batch layer
        verify(discovererA).insertDiscovery(flatTestData);
    }
}
