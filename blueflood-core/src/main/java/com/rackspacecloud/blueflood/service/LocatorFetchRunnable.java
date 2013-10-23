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

import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Range;
import com.netflix.astyanax.model.Column;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * fetches locators for a given slot and feeds a worker queue with rollup work. When those are all done notifies the
 * RollupService that slot can be removed from running.
  */
class LocatorFetchRunnable implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(LocatorFetchRunnable.class);
    private static final int LOCATOR_WAIT_FOR_ALL_SECS = 1000;
    
    private final ThreadPoolExecutor rollupExecutor;
    private final String parentSlotKey;
    private final ScheduleContext scheduleCtx;
    private final long serverTime;
    private static final Timer rollupLocatorExecuteTimer = Metrics.newTimer(RollupService.class, "Locate and Schedule Rollups for Slot", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);


    LocatorFetchRunnable(ScheduleContext scheduleCtx, String destSlotKey, ThreadPoolExecutor rollupExecutor) {
        this.rollupExecutor = rollupExecutor;
        this.parentSlotKey = destSlotKey;
        this.scheduleCtx = scheduleCtx;
        this.serverTime = scheduleCtx.getCurrentTimeMillis();
    }
    
    public void run() {
        final TimerContext timerCtx = rollupLocatorExecuteTimer.time();
        final Granularity gran = Granularity.granularityFromKey(parentSlotKey);
        final int parentSlot = Granularity.slotFromKey(parentSlotKey);
        final int shard = Granularity.shardFromKey(parentSlotKey);
        final Range parentRange = gran.deriveRange(parentSlot, serverTime);
        Granularity finerGran;

        try {
            finerGran = gran.finer();
        } catch (Exception ex) {
            log.error("No finer granularity available than " + gran);
            return;
        }

        if (log.isTraceEnabled())
            log.trace("Getting locators for {} {} @ {}", new Object[]{parentSlotKey, parentRange.toString(), scheduleCtx.getCurrentTimeMillis()});
        // todo: I can see this set becoming a memory hog.  There might be a better way of doing this.
        boolean success = true;
        long waitStart = System.currentTimeMillis();
        int rollCount = 0;

        final RollupExecutionContext executionContext = new RollupExecutionContext(Thread.currentThread());
        for (Column<Locator> locatorCol : AstyanaxReader.getInstance().getAllLocators(shard)) {
            final Locator locator = locatorCol.getName();

            if (log.isTraceEnabled())
                log.trace("Rolling up (check,metric,dimension) {} for (gran,slot,shard) {}", locatorCol.getName(), parentSlotKey);
            try {
                executionContext.increment();
                final RollupContext rollupContext = new RollupContext(locator, parentRange, finerGran);
                rollupExecutor.execute(new RollupRunnable(executionContext, rollupContext));
                rollCount += 1;
            } catch (Throwable any) {
                // continue on, but log the problem so that we can fix things later.
                executionContext.decrement();
                log.error(any.getMessage(), any);
                log.error("BasicRollup failed for {} at {}", parentSlotKey, serverTime);
            }
        }
        
        if (success) {
            // now wait until ctx is drained. someone needs to be notified.
            log.debug("Waiting for rollups to finish for " + parentSlotKey);
            while (!executionContext.done()) {
                try { 
                    Thread.currentThread().sleep(LOCATOR_WAIT_FOR_ALL_SECS); 
                } catch (InterruptedException ex) {
                    if (log.isTraceEnabled())
                        log.trace("Woken wile waiting for rollups to coalesce for {} {}", parentSlotKey);
                } finally {
                    log.debug("Still waiting for rollups to finish for {} {}", parentSlotKey, System.currentTimeMillis() - waitStart);
                }
            }
            if (log.isDebugEnabled())
                log.debug("Finished {} rollups for (gran,slot,shard) {} in {}", new Object[] {rollCount, parentSlotKey, System.currentTimeMillis() - waitStart});
            this.scheduleCtx.clearFromRunning(parentSlotKey);
        } else {
            log.warn("BasicRollup execution of {} failed.", gran);
            this.scheduleCtx.pushBackToScheduled(parentSlotKey);
        }

        timerCtx.stop();
    }
}
