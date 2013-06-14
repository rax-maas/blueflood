package com.cloudkick.blueflood.service;

import com.cloudkick.blueflood.rollup.Granularity;
import com.cloudkick.blueflood.types.Range;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Timer;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Will eventually become RollupContext as soon as the existing RollupContext is renamed to ScheduleContext.
 * This class keeps track of what is happening in an rollup for a specific metric.  Rollups for a single metric are run 
 * in parallel, as indicated by the counter.  The counter signals a thread that is waiting for all rollups for that
 * metric is finished so that the metric service can be signaled/
 */
class RollupContext {
    private final Granularity srcGran; // this is the source granularity (finer).
    private final Range range;
    private final AtomicInteger counter = new AtomicInteger(0);
    private final Thread owner;
    private final Timer executeTimer;
    private final Histogram waitHist;
    
    RollupContext(Range range, Granularity srcGran, Thread owner, Timer executeTimer, Histogram waitHist) {
        this.range = range;
        this.srcGran = srcGran;
        this.owner = owner;
        this.executeTimer = executeTimer;
        this.waitHist = waitHist;
    }
    
    void decrement() { 
        counter.decrementAndGet();
        owner.interrupt(); // this is what interrupts that long sleep in LocatorFetchRunnable.
    }
    
    void increment() { counter.incrementAndGet(); }
    boolean done() { return counter.get() == 0; }

    // dumb getters.
    
    Timer getExecuteTimer() { return executeTimer; }
    Histogram getWaitHist() { return waitHist; }
    Granularity getSourceGranularity() { return srcGran; }
    Range getRange() { return range; }
}
