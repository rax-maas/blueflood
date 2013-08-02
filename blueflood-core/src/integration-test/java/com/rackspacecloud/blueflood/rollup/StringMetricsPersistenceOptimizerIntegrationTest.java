package com.rackspacecloud.blueflood.rollup;

import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.TimeValue;
import com.netflix.astyanax.MutationBatch;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class StringMetricsPersistenceOptimizerIntegrationTest extends
        IntegrationTestBase {
    private MetricsPersistenceOptimizer metricsOptimizer;
    private Locator locator = Locator.createLocatorFromPathComponents("randomAccount", "randomEntity", "randomCheck", "randomDim", "randomMetric");
    private Locator otherLocator = Locator.createLocatorFromPathComponents("randomAccount", "randomEntity", "randomCheck", "randomBooleanMetric");

    @Before
    public void setUp() throws Exception {
        super.setUp();

        metricsOptimizer = new StringMetricsPersistenceOptimizer();
        populateMetricsFull();
    }

    /**
     * Writes some string metrics into Cassandra database
     */
    private void populateMetricsFull() throws Exception {
        final long collectionTimeInSecs = 12345;

        AstyanaxTester at = new AstyanaxTester();
        MutationBatch mb = at.createMutationBatch();
        mb.withRow(at.getStringCF(), locator)
                .putColumn(collectionTimeInSecs, "HTTP GET succeeded")
                .putColumn(collectionTimeInSecs + 1, "HTTP GET failed");
        mb.withRow(at.getStringCF(), otherLocator)
                .putColumn(collectionTimeInSecs + 2, "false");

        mb.execute();
    }

    @Test
    // Testing an edge case when there are no metrics available for a locator
    // in the database
    public void testShouldPersistForFirstInsertOfLocator() throws Exception {
        final Locator dummyLocator = Locator.createLocatorFromDbKey("acct.ent.check.dim.metric");
        final long collectionTimeInSecs = 45678;
        final String testMetric = "HTTP GET failed";
        final Metric newMetric = new Metric(dummyLocator, testMetric, collectionTimeInSecs,
                new TimeValue(2, TimeUnit.DAYS), "unknown");

        boolean shouldPersist = metricsOptimizer.shouldPersist(newMetric);

        // shouldPersist should return true as cassandra doesn't have any
        // metrics for this locator yet
        Assert.assertEquals(true, shouldPersist);
    }

    @Test
    public void testShouldPersistHappyCase() throws Exception {
        String testMetric = "HTTP GET failed";
        long collectionTimeInSecs = 56789;
        Metric newMetric = new Metric(locator, testMetric, collectionTimeInSecs,
                new TimeValue(2, TimeUnit.DAYS), "unknown");

        boolean shouldPersist = metricsOptimizer.shouldPersist(newMetric);

        // shouldPersist should be false as we have the same metric as the one
        // in the database
        Assert.assertEquals(false, shouldPersist);

        testMetric = "HTTP GET succeeded";
        collectionTimeInSecs++;
        newMetric = new Metric(locator, testMetric, collectionTimeInSecs,
                new TimeValue(2, TimeUnit.DAYS), "unknown");

        shouldPersist = metricsOptimizer.shouldPersist(newMetric);

        // shouldPersist should now be true as we do not have the same metric
        // as the one in the database
        Assert.assertEquals(true, shouldPersist);
    }

    @Test
    public void testShouldPersistForBooleanMetrics() throws Exception {
        boolean testMetric = false;
        long collectionTimeInSecs = 56789;
        Metric newMetric = new Metric(otherLocator, testMetric, collectionTimeInSecs,
                new TimeValue(2, TimeUnit.DAYS), "unknown");

        Assert.assertTrue(newMetric.isBoolean());

        boolean shouldPersist = metricsOptimizer.shouldPersist(newMetric);

        // shouldPersist should be false as we have the same metric as the one
        // in the database
        Assert.assertEquals(false, shouldPersist);

        testMetric = true;
        collectionTimeInSecs++;
        newMetric = new Metric(otherLocator, testMetric, collectionTimeInSecs,
                new TimeValue(2, TimeUnit.DAYS), "unknown");

        shouldPersist = metricsOptimizer.shouldPersist(newMetric);

        // shouldPersist should now be true as we do not have the same metric
        // as the one in the database
        Assert.assertEquals(true, shouldPersist);
    }
}
