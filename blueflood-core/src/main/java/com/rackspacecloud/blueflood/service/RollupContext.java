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
import com.yammer.metrics.core.Timer;

import java.util.concurrent.TimeUnit;
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
    private static final Timer executeTimer = Metrics.newTimer(RollupService.class, "Rollup Execution Timer", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private static final Histogram waitHist = Metrics.newHistogram(RollupService.class, "Rollup Wait Histogram", true);
    
    RollupContext(Range range, Granularity srcGran, Thread owner) {
        this.range = range;
        this.srcGran = srcGran;
        this.owner = owner;
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
