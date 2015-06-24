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

package com.rackspacecloud.blueflood.service;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.Util;
import org.junit.Assert;
import org.junit.Test;

// this test used to measure rollups that were refused, but we don't care anymore. Not it makes sure that all rollups
// get executed.
public class RollupThreadpoolIntegrationTest extends IntegrationTestBase {
    private static final Integer threadsInRollupPool = 2;

    static {
        // run this test with a configuration so that threadpool queue size is artificially constrained smaller.
        System.setProperty("MAX_ROLLUP_READ_THREADS", threadsInRollupPool.toString());
        System.setProperty("MAX_ROLLUP_WRITE_THREADS", threadsInRollupPool.toString());
    }

    @Test
    // remember: this tests behavior, not performance.
    public void testManyLocators() throws Exception {
        Assert.assertEquals(Configuration.getInstance().getIntegerProperty(CoreConfig.MAX_ROLLUP_READ_THREADS), threadsInRollupPool.intValue());
        int shardToTest = 0;

        // I want to see what happens when RollupService.rollupExecutors gets too much work. It should never reject
        // work.
        long time = 1234;

        // now we need to put data that will generate an enormous amount of locators.
        AstyanaxWriter writer = AstyanaxWriter.getInstance();

        final int NUM_LOCATORS = 5000;
        int locatorCount = 0;
        while (locatorCount < NUM_LOCATORS) {
            // generate 100 random metrics.
            writer.insertFull(makeRandomIntMetrics(100));
            locatorCount += 100;
        }

        // lets see how many locators this generated. We want it to be a lot.

        int locatorsForTestShard = 0;
        for (Locator locator : AstyanaxReader.getInstance().getLocatorsToRollup(shardToTest)) {
            locatorsForTestShard++;
        }

        // Make sure number of locators for test shard is greater than number of rollup threads.
        // This is required so that rollups would be rejected for some locators.
        Assert.assertTrue(threadsInRollupPool < locatorsForTestShard);

        // great. now lets schedule those puppies.
        ScheduleContext ctx = new ScheduleContext(time, Util.parseShards(String.valueOf(shardToTest)));
        RollupService rollupService = new RollupService(ctx);
        rollupService.setKeepingServerTime(false);

        // indicate arrival (which we forced using Writer).
        ctx.update(time, shardToTest);

        // move time forward
        time += 500000;
        ctx.setCurrentTimeMillis(time);

        // start the rollups.
        Thread rollupThread = new Thread(rollupService, "rollup service test");
        rollupThread.start();

        Class.forName("com.rackspacecloud.blueflood.service.SingleRollupReadContext"); // Static initializer for the metric

        MetricRegistry registry = Metrics.getRegistry();
        Timer rollupsTimer = registry.getTimers().get(MetricRegistry.name(RollupService.class, "Rollup Execution Timer"));
//        Timer rollupsTimer = (Timer)registry.allMetrics().get(new MetricName("com.rackspacecloud.blueflood.service", "RollupService", "Rollup Execution Timer"));

        Assert.assertNotNull(rollupsTimer);

        // wait up to 120s for those rollups to finish.
        long start = System.currentTimeMillis();
        while (true) {
            try { Thread.currentThread().sleep(1000); } catch (Exception ex) { }
            if (rollupsTimer.getCount() >= locatorsForTestShard)
                break;
            Assert.assertTrue(String.format("rollups:%d", rollupsTimer.getCount()), System.currentTimeMillis() - start < 120000);
        }

        // make sure there were some that were delayed. If not, we need to increase NUM_LOCATORS.
        Assert.assertTrue(rollupsTimer.getCount() > 0);
    }
}
