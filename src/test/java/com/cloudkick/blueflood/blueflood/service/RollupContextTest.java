package com.cloudkick.blueflood.service;

import com.cloudkick.blueflood.rollup.Granularity;
import com.cloudkick.blueflood.types.Range;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

public class RollupContextTest extends TestCase{

    public void testRollupContextConstructor() {
        Range myRange = new Range(0, 300000);
        Granularity gran = Granularity.MIN_5;
        Thread myThread = new Thread();
        Timer myTimer = Metrics.newTimer(RollupService.class, "Rollup Execution Timer", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        MetricsRegistry REGISTRY = Metrics.defaultRegistry();
        Histogram myHistogram = REGISTRY.newHistogram(RollupContext.class, "Rollup-Histogram", false);

        RollupContext myRollupContext = new RollupContext(myRange, gran, myThread, myTimer, myHistogram);

        assertNotNull(myRollupContext);
        assertTrue(myRollupContext.done());
        myRollupContext.increment();
        assertFalse(myRollupContext.done());
        myRollupContext.decrement();
        assertTrue(myRollupContext.done());

        assertEquals(myTimer, myRollupContext.getExecuteTimer());
        assertEquals(myHistogram, myRollupContext.getWaitHist());
        assertEquals(gran, myRollupContext.getSourceGranularity());
        assertEquals(myRange, myRollupContext.getRange());

    }
}
