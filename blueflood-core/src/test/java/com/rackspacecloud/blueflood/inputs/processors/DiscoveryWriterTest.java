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
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
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

    @Test
    public void testProcessorThrottlesNewMetricsDiscoveryGlobally() throws Exception {
        // Given a DiscoveryWriter that throttles new locators to one per minute
        DiscoveryWriter writer = new DiscoveryWriter(new ThreadPoolBuilder()
                .withName("Metric Discovery Writing")
                .withCorePoolSize(10)
                .withMaxPoolSize(10)
                .withUnboundedQueue()
                .withRejectedHandler(new ThreadPoolExecutor.AbortPolicy())
                .build());
        DiscoveryIO discoveryIO = mock(DiscoveryIO.class);
        writer.registerIO(discoveryIO);
        writer.setMaxNewLocatorsPerMinute(1);

        // When I process some new metrics
        Locator locator1 = Locator.createLocatorFromPathComponents("t1", "foo");
        Locator locator2 = Locator.createLocatorFromPathComponents("t2", "foo");
        List<IMetric> metrics = new ArrayList<>();
        metrics.add(randomMetric(locator1));
        metrics.add(randomMetric(locator2));
        writer.processMetrics(Collections.singletonList(metrics)).get();

        // Then all the metrics should be sent to the discovery IO - throttling doesn't kick in mid-batch
        assertDiscoveryReceivedMetrics(discoveryIO, 1, metrics);

        // When I process more new metrics from both old and new tenants
        Locator locator3 = Locator.createLocatorFromPathComponents("t1", "bar");
        Locator locator4 = Locator.createLocatorFromPathComponents("t3", "foo");
        List<IMetric> oldTenantMetrics = new ArrayList<>();
        oldTenantMetrics.add(randomMetric(locator3));
        oldTenantMetrics.add(randomMetric(locator3));
        List<IMetric> newTenantMetrics = new ArrayList<>();
        newTenantMetrics.add(randomMetric(locator4));
        newTenantMetrics.add(randomMetric(locator4));
        writer.processMetrics(Arrays.asList(oldTenantMetrics, newTenantMetrics)).get();

        // Then the global throttling prevents any new locators from being sent to the discovery IO
        verify(discoveryIO, times(1)).insertDiscovery(anyList());
    }

    @Test
    public void testProcessorThrottlesNewMetricDiscoveryPerTenant() throws Exception {
        // Given a DiscoveryWriter that throttles new locators to one per minute per tenant
        DiscoveryWriter writer = new DiscoveryWriter(new ThreadPoolBuilder()
                .withName("Metric Discovery Writing")
                .withCorePoolSize(10)
                .withMaxPoolSize(10)
                .withUnboundedQueue()
                .withRejectedHandler(new ThreadPoolExecutor.AbortPolicy())
                .build());
        DiscoveryIO discoveryIO = mock(DiscoveryIO.class);
        writer.registerIO(discoveryIO);
        writer.setMaxNewLocatorsPerMinutePerTenant(1);

        // When I process some new metrics from a couple of tenants
        Locator locator1 = Locator.createLocatorFromPathComponents("t1", "foo");
        Locator locator2 = Locator.createLocatorFromPathComponents("t1", "bar");
        Locator locator3 = Locator.createLocatorFromPathComponents("t2", "foo");
        Locator[] locators = new Locator[]{locator1, locator2, locator3};
        List<IMetric> firstMetrics = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            firstMetrics.add(randomMetric(locators[i % 3]));
        }
        writer.processMetrics(Collections.singletonList(firstMetrics)).get();

        // Then all the metrics should be sent to the discovery IO - throttling doesn't kick in mid-batch
        assertDiscoveryReceivedMetrics(discoveryIO, 1, firstMetrics);

        // When I process more new metrics from both old and new tenants
        Locator locator4 = Locator.createLocatorFromPathComponents("t2", "bar");
        Locator locator5 = Locator.createLocatorFromPathComponents("t3", "foo");
        Locator locator6 = Locator.createLocatorFromPathComponents("t4", "foo");
        List<IMetric> oldTenantMetrics = new ArrayList<>();
        oldTenantMetrics.add(randomMetric(locator4));
        oldTenantMetrics.add(randomMetric(locator4));
        List<IMetric> newTenantMetrics = new ArrayList<>();
        newTenantMetrics.add(randomMetric(locator5));
        newTenantMetrics.add(randomMetric(locator5));
        newTenantMetrics.add(randomMetric(locator6));
        newTenantMetrics.add(randomMetric(locator6));
        writer.processMetrics(Arrays.asList(oldTenantMetrics, newTenantMetrics)).get();

        // Then throttling prevents new metrics from old tenants, so only new tenant metrics are sent to the discovery
        // IO
        assertDiscoveryReceivedMetrics(discoveryIO, 2, newTenantMetrics);
    }

    @Test
    public void testProcessorThrottlesNewMetricDiscoveryPerTenantAndGlobally() throws Exception {
        // Given a DiscoveryWriter that throttles new locators to one per minute per tenant and to four globally
        DiscoveryWriter writer = new DiscoveryWriter(new ThreadPoolBuilder()
                .withName("Metric Discovery Writing")
                .withCorePoolSize(10)
                .withMaxPoolSize(10)
                .withUnboundedQueue()
                .withRejectedHandler(new ThreadPoolExecutor.AbortPolicy())
                .build());
        DiscoveryIO discoveryIO = mock(DiscoveryIO.class);
        writer.registerIO(discoveryIO);
        writer.setMaxNewLocatorsPerMinute(4);
        writer.setMaxNewLocatorsPerMinutePerTenant(1);

        // When I process some new metrics from a couple of tenants
        Locator locator1 = Locator.createLocatorFromPathComponents("t1", "foo");
        Locator locator2 = Locator.createLocatorFromPathComponents("t1", "bar");
        Locator locator3 = Locator.createLocatorFromPathComponents("t2", "foo");
        Locator[] locators = new Locator[]{locator1, locator2, locator3};
        List<IMetric> firstMetrics = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            firstMetrics.add(randomMetric(locators[i % 3]));
        }
        writer.processMetrics(Collections.singletonList(firstMetrics)).get();

        // Then all the metrics should be sent to the discovery IO - throttling doesn't kick in mid-batch
        // (running count: three locators from two tenants)
        assertDiscoveryReceivedMetrics(discoveryIO, 1, firstMetrics);

        // When I process more metrics from a mix of old and new tenants
        Locator locator4 = Locator.createLocatorFromPathComponents("t2", "bar");
        Locator locator5 = Locator.createLocatorFromPathComponents("t3", "foo");
        Locator locator6 = Locator.createLocatorFromPathComponents("t4", "foo");
        List<IMetric> oldTenantMetrics = new ArrayList<>();
        oldTenantMetrics.add(randomMetric(locator4));
        oldTenantMetrics.add(randomMetric(locator4));
        List<IMetric> newTenantMetrics = new ArrayList<>();
        newTenantMetrics.add(randomMetric(locator5));
        newTenantMetrics.add(randomMetric(locator5));
        newTenantMetrics.add(randomMetric(locator6));
        newTenantMetrics.add(randomMetric(locator6));
        writer.processMetrics(Arrays.asList(oldTenantMetrics, newTenantMetrics)).get();

        // Then per-tenant throttling prevents new metrics from old tenants; new tenant metrics all go through
        // (running count: five locators from four tenants)
        assertDiscoveryReceivedMetrics(discoveryIO, 2, newTenantMetrics);

        // When I try to send any more metrics
        List<IMetric> moreMetrics = new ArrayList<>();
        moreMetrics.add(randomMetric(locator1));
        moreMetrics.add(randomMetric(locator2));
        moreMetrics.add(randomMetric(locator3));
        moreMetrics.add(randomMetric(locator4));
        moreMetrics.add(randomMetric(locator5));
        moreMetrics.add(randomMetric(locator6));
        moreMetrics.add(randomMetric(Locator.createLocatorFromPathComponents("t5", "foo")));
        writer.processMetrics(Collections.singletonList(moreMetrics)).get();

        // Then the discovery IO was only called the two times because the global throttle has been exceeded
        // Tenant five is out of luck!
        verify(discoveryIO, times(2)).insertDiscovery(anyList());
    }

    /**
     * Verifies that a DiscoveryIO has been called a given number of times and that the last call was with a given list
     * of metrics.
     */
    private void assertDiscoveryReceivedMetrics(DiscoveryIO discoveryIO, int callNumber, List<IMetric> expected) throws Exception {
        ArgumentCaptor<List> metricsCaptor = ArgumentCaptor.forClass(List.class);
        verify(discoveryIO, times(callNumber)).insertDiscovery(metricsCaptor.capture());
        List<IMetric> insertedMetrics = metricsCaptor.getAllValues().get(callNumber - 1);
        assertThat(insertedMetrics.size(), is(expected.size()));
        assertThat(expected, equalTo(insertedMetrics));
    }

    private final Random random = new Random();
    private IMetric randomMetric(Locator locator) {
        return new Metric(locator, random.nextInt(1000), random.nextInt(1000), new TimeValue(10, TimeUnit.SECONDS), "test unit");
    }
}
