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

import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.shallows.EmptyColumnList;
import com.rackspacecloud.blueflood.io.AstyanaxIO;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Range;
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
    
    private final ThreadPoolExecutor rollupReadExecutor;
    private final ThreadPoolExecutor rollupWriteExecutor;
    private final String parentSlotKey;
    private final ScheduleContext scheduleCtx;
    private final long serverTime;
    private static final Timer rollupLocatorExecuteTimer = Metrics.newTimer(RollupService.class, "Locate and Schedule Rollups for Slot", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);


    LocatorFetchRunnable(ScheduleContext scheduleCtx, String destSlotKey, ThreadPoolExecutor rollupReadExecutor, ThreadPoolExecutor rollupWriteExecutor) {
        this.rollupReadExecutor = rollupReadExecutor;
        this.rollupWriteExecutor = rollupWriteExecutor;
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
        long waitStart = System.currentTimeMillis();
        int rollCount = 0;

        final RollupExecutionContext executionContext = new RollupExecutionContext(Thread.currentThread());
        final RollupBatchWriter rollupBatchWriter = new RollupBatchWriter(rollupWriteExecutor, executionContext);
        ColumnList<Locator> locators = new EmptyColumnList<Locator>();
        try {
            locators = AstyanaxReader.getInstance().getAllLocators(shard);
        } catch (RuntimeException e) {
            executionContext.markUnsuccessful(e);
            log.error("Failed reading locators for slot: " + parentSlot, e);
        }
        for (Column<Locator> locatorCol : locators) {
            final Locator locator = locatorCol.getName();

            if (log.isTraceEnabled())
                log.trace("Rolling up (check,metric,dimension) {} for (gran,slot,shard) {}", locatorCol.getName(), parentSlotKey);
            try {
                executionContext.incrementReadCounter();
                final SingleRollupReadContext singleRollupReadContext = new SingleRollupReadContext(locator, parentRange, finerGran);
                rollupReadExecutor.execute(new RollupRunnable(executionContext, singleRollupReadContext, rollupBatchWriter));
                rollCount += 1;
            } catch (Throwable any) {
                // continue on, but log the problem so that we can fix things later.
                executionContext.markUnsuccessful(any);
                executionContext.decrementReadCounter();
                log.error(any.getMessage(), any);
                log.error("BasicRollup failed for {} at {}", parentSlotKey, serverTime);
            }
        }
        
        // now wait until ctx is drained. someone needs to be notified.
        log.debug("Waiting for rollups to finish for " + parentSlotKey);
        while (!executionContext.doneReading() || !executionContext.doneWriting()) {
            if (executionContext.doneReading()) {
                rollupBatchWriter.drainBatch(); // gets any remaining rollups enqueued for write. should be no-op after being called once
            }
            try {
                Thread.currentThread().sleep(LOCATOR_WAIT_FOR_ALL_SECS);
            } catch (InterruptedException ex) {
                if (log.isTraceEnabled())
                    log.trace("Woken wile waiting for rollups to coalesce for {} {}", parentSlotKey);
            } finally {
                String verb = executionContext.doneReading() ? "writing" : "reading";
                log.debug("Still waiting for rollups to finish {} for {} {}", new Object[] {verb, parentSlotKey, System.currentTimeMillis() - waitStart });
            }
        }
        if (log.isDebugEnabled())
            log.debug("Finished {} rollups for (gran,slot,shard) {} in {}", new Object[] {rollCount, parentSlotKey, System.currentTimeMillis() - waitStart});

        if (executionContext.wasSuccessful()) {
            this.scheduleCtx.clearFromRunning(parentSlotKey);
        } else {
            log.error("Performing BasicRollups for {} failed", parentSlotKey);
            this.scheduleCtx.pushBackToScheduled(parentSlotKey);
        }

        timerCtx.stop();
    }
}
