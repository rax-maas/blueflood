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

import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.Range;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class RollupContextTest {

    @Test
    public void testRollupContextConstructor() {
        Range myRange = new Range(0, 300000);
        Granularity gran = Granularity.MIN_5;
        Thread myThread = new Thread();
        Timer myTimer = Metrics.newTimer(RollupService.class, "Rollup Execution Timer", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        MetricsRegistry REGISTRY = Metrics.defaultRegistry();
        Histogram myHistogram = REGISTRY.newHistogram(RollupService.class, "Rollup Wait Histogram", true);

        RollupContext myRollupContext = new RollupContext(myRange, gran, myThread);

        Assert.assertNotNull(myRollupContext);
        Assert.assertTrue(myRollupContext.done());
        myRollupContext.increment();
        Assert.assertFalse(myRollupContext.done());
        myRollupContext.decrement();
        Assert.assertTrue(myRollupContext.done());

        Assert.assertEquals(myTimer, myRollupContext.getExecuteTimer());
        Assert.assertEquals(myHistogram, myRollupContext.getWaitHist());
        Assert.assertEquals(gran, myRollupContext.getSourceGranularity());
        Assert.assertEquals(myRange, myRollupContext.getRange());
    }
}
