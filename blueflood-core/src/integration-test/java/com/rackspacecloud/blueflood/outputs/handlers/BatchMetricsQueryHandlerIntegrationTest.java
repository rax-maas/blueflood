/*
 * Copyright 2013 Rackspace
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

package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.IncomingMetricMetadataAnalyzer;
import com.rackspacecloud.blueflood.types.BatchMetricsQuery;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.Range;
import com.rackspacecloud.blueflood.utils.RollupTestUtils;
import com.rackspacecloud.blueflood.utils.TimeValue;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class BatchMetricsQueryHandlerIntegrationTest extends IntegrationTestBase {
    private final long baseMillis = 1335820166000L;
    private final String tenantId = "ac" + IntegrationTestBase.randString(8);
    private final String metricName = "met_" + IntegrationTestBase.randString(8);
    private final String strMetricName = "strMet_" + IntegrationTestBase.randString(8);
    final List<Locator> locators = new ArrayList<Locator>() {{
            add(Locator.createLocatorFromPathComponents(tenantId, metricName));
            add(Locator.createLocatorFromPathComponents(tenantId, strMetricName));
    }};
    private final Map<Locator, Map<Granularity, Integer>> answers = new HashMap<Locator, Map<Granularity,Integer>>();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        AstyanaxWriter writer = AstyanaxWriter.getInstance();
        IncomingMetricMetadataAnalyzer analyzer = new IncomingMetricMetadataAnalyzer(MetadataCache.getInstance());

        // insert something every 1m for 24h
        for (int i = 0; i < 1440; i++) {
            final long curMillis = baseMillis + i * 60000;
            final List<Metric> metrics = new ArrayList<Metric>();
            final Metric metric = getRandomIntMetric(locators.get(0), curMillis);
            final Metric stringMetric = getRandomStringmetric(locators.get(1), curMillis);
            metrics.add(metric);
            metrics.add(stringMetric);

            analyzer.scanMetrics(metrics);
            writer.insertFull(metrics);
        }

        // generate every level of rollup for the raw data
        Granularity g = Granularity.FULL;
        while (g != Granularity.MIN_1440) {
            g = g.coarser();
            for (Locator locator : locators) {
                RollupTestUtils.generateRollups(locator, baseMillis, baseMillis + 86400000, g);
            }
        }

        final Map<Granularity, Integer> answerForNumericMetric = new HashMap<Granularity, Integer>();
        answerForNumericMetric.put(Granularity.FULL, 1440);
        answerForNumericMetric.put(Granularity.MIN_5, 289);
        answerForNumericMetric.put(Granularity.MIN_20, 73);
        answerForNumericMetric.put(Granularity.MIN_60, 25);
        answerForNumericMetric.put(Granularity.MIN_240, 7);
        answerForNumericMetric.put(Granularity.MIN_1440, 2);

        final Map<Granularity, Integer> answerForStringMetric = new HashMap<Granularity, Integer>();
        answerForStringMetric.put(Granularity.FULL, 1440);
        answerForStringMetric.put(Granularity.MIN_5, 1440);
        answerForStringMetric.put(Granularity.MIN_20, 1440);
        answerForStringMetric.put(Granularity.MIN_60, 1440);
        answerForStringMetric.put(Granularity.MIN_240, 1440);
        answerForStringMetric.put(Granularity.MIN_1440, 1440);

        answers.put(locators.get(0), answerForNumericMetric);
        answers.put(locators.get(1), answerForStringMetric);
    }

    @Test
    public void testBatchGet() throws Exception {
        ThreadPoolExecutor executor = new ThreadPoolBuilder().withBoundedQueue(10)
                .withCorePoolSize(1).withMaxPoolSize(1).withName("TestBatchQuery").build();
        BatchMetricsQueryHandler batchMetricsQueryHandler = new BatchMetricsQueryHandler(executor,
                AstyanaxReader.getInstance());

        Granularity gran = Granularity.MIN_20;
        Range range = new Range(gran.snapMillis(baseMillis), baseMillis + 86400000);

        final List<Locator> tooManyLocators = new ArrayList<Locator>();
        tooManyLocators.addAll(locators);
        // generate arbitrarily large number of locators so we slow down the query so we can test a timeout
        for (int i = 0; i < 50; i++) {
            tooManyLocators.add(Locator.createLocatorFromDbKey(UUID.randomUUID().toString()));
        }
        BatchMetricsQuery query = new BatchMetricsQuery(tooManyLocators, range, gran);

        // Now test a bad case with extremely low timeout. We shouldn't throw any exceptions.
        Map<Locator, MetricData> results = batchMetricsQueryHandler.execute(query, new TimeValue(1, TimeUnit.MILLISECONDS));
        // Make sure there were things still in progress and nothing breaks.
        Assert.assertNull(results);
        // Executor queue should not have any items left.
        // XXX: OpenJDK6 ArrayBlockingQueue has a weird bug where it returns negative value for size() when you call
        // purge() on the executor that uses the queue.
        Assert.assertTrue("Number of items left in queue should be 0", executor.getQueue().size() <= 0);
        // Note there is no guarantee that items currently in execution will definitely be done or interrupted.

        // Test happy case. 5s is plenty of time to read two metrics.
        // Use the same executor. We shouldn't see issues.
        results = batchMetricsQueryHandler.execute(query, new TimeValue(5, TimeUnit.SECONDS));
        Assert.assertEquals(locators.size(), results.size());

        for (Map.Entry<Locator, MetricData> item : results.entrySet()) {
            MetricData data = item.getValue();
            Assert.assertEquals((int) answers.get(item.getKey()).get(gran), data.getData().getPoints().size());
        }
    }
}
