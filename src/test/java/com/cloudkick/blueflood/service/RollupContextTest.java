package com.cloudkick.blueflood.service;

import com.cloudkick.blueflood.rollup.Granularity;
import com.cloudkick.blueflood.types.Range;
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
