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

import com.codahale.metrics.Timer;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Range;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Fetches locators for a given {@link SlotKey}, feeds a worker queue with rollup work.
 * Upon completion, notifies the {@link RollupService} that the given slot check has finished.
 */
class SlotCheckRunnable implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(SlotCheckRunnable.class);
    private static final int LOCATOR_WAIT_FOR_ALL_SECS = 1000;

    /**
     * Thread pool to read higher-resolution, unrolled metrics.
     */
    private final ThreadPoolExecutor rollupReadExecutor;
    /**
     * Thread pool to write lower-resolution, rolled metrics.
     */
    private final ThreadPoolExecutor rollupWriteExecutor;
    /**
     * Slot key of the parent (higher resolution) data being rolled.
     */
    private final SlotKey parentSlotKey;
    /**
     * Used to re-schedule or de-schedule the rollups.
     */
    private final SlotCheckScheduler scheduleCtx;
    /**
     * start time of the rollups. Used to deduce the current time range for the slot.
     */
    private final long serverTime;

    private static final Timer rollupLocatorExecuteTimer = Metrics.timer(RollupService.class, "Locate and Schedule Rollups for Slot");
    /**
     * If set, histogram rollups are enabled.
     */
    private static final boolean enableHistograms = Configuration.getInstance().
            getBooleanProperty(CoreConfig.ENABLE_HISTOGRAMS);

    SlotCheckRunnable(
            long serverTime,
            SlotCheckScheduler scheduleCtx,
            SlotKey destSlotKey,
            ThreadPoolExecutor rollupReadExecutor,
            ThreadPoolExecutor rollupWriteExecutor) {
        this.rollupReadExecutor = rollupReadExecutor;
        this.rollupWriteExecutor = rollupWriteExecutor;
        this.parentSlotKey = destSlotKey;
        this.scheduleCtx = scheduleCtx;
        this.serverTime = serverTime;
    }
    
    public void run() {
        final Timer.Context timerCtx = rollupLocatorExecuteTimer.time();
        try {
            boolean success = performSlotCheck();
            if (success) {
                this.scheduleCtx.markFinished(parentSlotKey);
            } else {
                log.error("Performing BasicRollups for {} failed", parentSlotKey);
                this.scheduleCtx.reschedule(parentSlotKey, false);
            }
        } finally {
            timerCtx.stop();
        }
    }

    /**
     * Perform a single slot check.
     * @return true if the slot check is successful, false otherwise.
     */
    private boolean performSlotCheck() {
        final Granularity gran = parentSlotKey.getGranularity();
        final int parentSlot = parentSlotKey.getSlot();
        final int shard = parentSlotKey.getShard();
        final Range parentRange = gran.deriveRange(parentSlot, serverTime);

        try {
            gran.finer();
        } catch (Exception ex) {
            log.error("No finer granularity available than {}.", gran);
            return true;
        }

        if (log.isTraceEnabled()) {
            log.trace("Getting locators for {} {} @ {}", new Object[]{parentSlotKey, parentRange.toString(), serverTime});
        }

        long waitStart = System.currentTimeMillis();
        int rollCount = 0;

        final RollupExecutionContext executionContext = new RollupExecutionContext(Thread.currentThread());
        final RollupBatchWriter rollupBatchWriter = new RollupBatchWriter(rollupWriteExecutor, executionContext);
        // todo: I can see this set becoming a memory hog.  There might be a better way of doing this.
        Set<Locator> locators = new HashSet<Locator>();

        // if the slot is too old, then drop it on the floor.
        try {
            locators.addAll(AstyanaxReader.getInstance().getLocatorsToRollup(shard));
        } catch (RuntimeException e) {
            executionContext.markUnsuccessful(e);
            log.error("Failed reading locators for slot: " + parentSlot, e);
        }

        for (Locator locator : locators) {
            if (log.isTraceEnabled())
                log.trace("Rolling up (check,metric,dimension) {} for (gran,slot,shard) {}", locator, parentSlotKey);
            try {
                executionContext.incrementReadCounter();
                final SingleRollupReadContext singleRollupReadContext = new SingleRollupReadContext(locator, parentRange, gran);
                rollupReadExecutor.execute(new RollupRunnable(executionContext, singleRollupReadContext, rollupBatchWriter));
                rollCount += 1;
            } catch (Exception any) {
                // continue on, but log the problem so that we can fix things later.
                executionContext.markUnsuccessful(any);
                executionContext.decrementReadCounter();
                log.error(any.getMessage(), any);
                log.error("BasicRollup failed for {} at {}", parentSlotKey, serverTime);
            }

            if (enableHistograms) {
                // Also, compute histograms. Histograms for > 5 MIN granularity are always computed from 5 MIN histograms.
                try {
                    executionContext.incrementReadCounter();
                    final SingleRollupReadContext singleRollupReadContext = new SingleRollupReadContext(locator,
                            parentRange, gran);
                    rollupReadExecutor.execute(new HistogramRollupRunnable(executionContext, singleRollupReadContext,
                            rollupBatchWriter));
                    rollCount += 1;
                } catch (RejectedExecutionException ex) {
                    executionContext.markUnsuccessful(ex); // We should ideally only recompute the failed locators alone.
                    executionContext.decrementReadCounter();
                    log.error("Histogram rollup rejected for {} at {}", parentSlotKey, serverTime);
                    log.error("Exception: ", ex);
                } catch (Exception ex) { // do not retrigger rollups when they fail. histograms are fine to be lost.
                    executionContext.decrementReadCounter();
                    log.error("Histogram rollup rejected for {} at {}", parentSlotKey, serverTime);
                    log.error("Exception: ", ex);
                }
            }
        }
        
        // now wait until ctx is drained. someone needs to be notified.
        log.debug("Waiting for rollups to finish for {}.", parentSlotKey);
        while (!executionContext.doneReading() || !executionContext.doneWriting()) {
            if (executionContext.doneReading()) {
                rollupBatchWriter.drainBatch(); // gets any remaining rollups enqueued for put. should be no-op after being called once
            }
            try {
                Thread.sleep(LOCATOR_WAIT_FOR_ALL_SECS * 1000);
            } catch (InterruptedException ex) {
                if (log.isTraceEnabled())
                    log.trace("Woken wile waiting for rollups to coalesce for {}.", parentSlotKey);
            } finally {
                String verb = executionContext.doneReading() ? "writing" : "reading";
                log.debug("Still waiting for rollups to finish {} for {} {}", new Object[] {verb, parentSlotKey, System.currentTimeMillis() - waitStart });
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Finished {} rollups for (gran,slot,shard) {} in {}", new Object[]{rollCount, parentSlotKey, System.currentTimeMillis() - waitStart});
        }

        return executionContext.wasSuccessful();
    }
}
