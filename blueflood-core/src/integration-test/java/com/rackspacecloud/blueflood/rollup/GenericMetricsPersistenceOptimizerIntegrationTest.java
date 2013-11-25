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

package com.rackspacecloud.blueflood.rollup;

import com.rackspacecloud.blueflood.io.AstyanaxIO;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.io.NumericSerializer;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.TimeValue;
import com.netflix.astyanax.MutationBatch;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class GenericMetricsPersistenceOptimizerIntegrationTest extends
        IntegrationTestBase {
    private MetricsPersistenceOptimizer metricsOptimizer;
    private Locator locator = Locator.createLocatorFromPathComponents("randomAccount", "randomEntity", "randomCheck", "randomDim", "randomMetric");

    @Before
    public void setUp() throws Exception {
        super.setUp();

        metricsOptimizer = new GenericMetricsPersistenceOptimizer();
        populateMetricsFull();
    }

    /**
     * Writes some INT32 metrics into Cassandra database
     */
    private void populateMetricsFull() throws Exception {
        final long collectionTimeInSecs = 12345;

        // add a metric to locator
        AstyanaxTester at = new AstyanaxTester();
        MutationBatch mb = at.createMutationBatch();
        mb.withRow(at.getFullCF(), locator)
                .putColumn(collectionTimeInSecs,
                        NumericSerializer.serializerFor(Integer.class).toByteBuffer(123));

        // add another metric
        mb.withRow(at.getFullCF(), locator)
                .putColumn(collectionTimeInSecs + 1,
                        NumericSerializer.serializerFor(Integer.class).toByteBuffer(456));

        mb.execute();
    }

    @Test
    // Testing an edge case when there are no metrics available for a locator
    // in the database
    public void testShouldPersistForFirstInsertOfLocator() throws Exception {
        final Locator dummyLocator = Locator.createLocatorFromPathComponents("acctId", "entityId", "checkId", "dim", "metric");
        final long collectionTimeInSecs = 45678;
        final int testMetric = 789;
        final Metric newMetric = new Metric(dummyLocator, testMetric, collectionTimeInSecs,
                new TimeValue(2, TimeUnit.DAYS), "unknown");

        boolean shouldPersist = metricsOptimizer.shouldPersist(newMetric);

        // shouldPersist should return true as cassandra doesn't have any
        // metrics for this locator yet
        Assert.assertEquals(true, shouldPersist);
    }

    @Test
    public void testShouldPersistHappyCase() throws Exception {
        int testMetric = 123;
        long collectionTimeInSecs = 56789;
        Metric newMetric = new Metric(locator, testMetric, collectionTimeInSecs,
                new TimeValue(2, TimeUnit.DAYS), "unknown");

        boolean shouldPersist = metricsOptimizer.shouldPersist(newMetric);

        // shouldPersist should be true as for non-string metrics we don't
        // care
        Assert.assertEquals(true, shouldPersist);

        testMetric = 789;
        collectionTimeInSecs++;
        final Metric newerMetric = new Metric(locator, testMetric, collectionTimeInSecs,
                new TimeValue(2, TimeUnit.DAYS), "unknown");

        shouldPersist = metricsOptimizer.shouldPersist(newerMetric);

        // shouldPersist should now be true as we do not have the same metric
        // as the one in the database
        Assert.assertEquals(true, shouldPersist);
    }
}
