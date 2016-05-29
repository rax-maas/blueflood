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
import com.google.common.annotations.VisibleForTesting;
import com.rackspacecloud.blueflood.io.IOContainer;
import com.rackspacecloud.blueflood.io.Instrumentation;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Range;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * fetches locators for a given slot and feeds a worker queue with rollup work. When those are all done notifies the
 * RollupService that slot can be removed from running.
  */
class LocatorFetchRunnable implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(LocatorFetchRunnable.class);
    private static final int LOCATOR_WAIT_FOR_ALL_SECS = 1000;
    
    private ExecutorService rollupReadExecutor;
    private ThreadPoolExecutor rollupWriteExecutor;
    private ExecutorService enumValidatorExecutor;
    private SlotKey parentSlotKey;
    private ScheduleContext scheduleCtx;
    private long serverTime;
    private static final Timer rollupLocatorExecuteTimer = Metrics.timer(RollupService.class, "Locate and Schedule Rollups for Slot");

    private Range parentRange;

    private final boolean IS_REROLL_ONLY_DELAYED_METRICS = Configuration.getInstance().getBooleanProperty(CoreConfig.REROLL_ONLY_DELAYED_METRICS);

    LocatorFetchRunnable(ScheduleContext scheduleCtx,
                         SlotKey destSlotKey,
                         ExecutorService rollupReadExecutor,
                         ThreadPoolExecutor rollupWriteExecutor,
                         ExecutorService enumValidatorExecutor) {

        initialize(scheduleCtx, destSlotKey, rollupReadExecutor,
                rollupWriteExecutor, enumValidatorExecutor);
    }

    @VisibleForTesting
    public void initialize(ScheduleContext scheduleCtx,
                           SlotKey destSlotKey,
                           ExecutorService rollupReadExecutor,
                           ThreadPoolExecutor rollupWriteExecutor,
                           ExecutorService enumValidatorExecutor) {

        this.rollupReadExecutor = rollupReadExecutor;
        this.rollupWriteExecutor = rollupWriteExecutor;
        this.parentSlotKey = destSlotKey;
        this.scheduleCtx = scheduleCtx;
        this.serverTime = scheduleCtx.getCurrentTimeMillis();
        this.enumValidatorExecutor = enumValidatorExecutor;
        this.parentRange = getGranularity().deriveRange(getParentSlot(), serverTime);
    }

    protected Granularity getGranularity() { return parentSlotKey.getGranularity(); }
    protected int getParentSlot() { return parentSlotKey.getSlot(); }
    protected int getShard() { return parentSlotKey.getShard(); }

    public void run() {
        final Timer.Context timerCtx = rollupLocatorExecuteTimer.time();

        try {
            getGranularity().finer();
        } catch (Exception ex) {
            log.error("No finer granularity available than " + getGranularity());
            return;
        }

        if (log.isTraceEnabled())
            log.trace("Getting locators for {} {} @ {}", new Object[]{parentSlotKey, parentRange.toString(), scheduleCtx.getCurrentTimeMillis()});
        // todo: I can see this set becoming a memory hog.  There might be a better way of doing this.
        long waitStart = System.currentTimeMillis();
        int rollCount = 0;

        final RollupExecutionContext executionContext = createRollupExecutionContext();
        final RollupBatchWriter rollupBatchWriter = createRollupBatchWriter(executionContext);

        Set<Locator> locators = getLocators(executionContext);

        boolean isSlotBeingRerolled = scheduleCtx.isReroll(parentSlotKey);
        for (Locator locator : locators) {

            //if slot is being re-rolled, only process locator if locator's lastUpdateTimestamp is after last rollup time
            if (IS_REROLL_ONLY_DELAYED_METRICS &&
                    isSlotBeingRerolled) {

                UpdateStamp updateStamp = scheduleCtx.getShardStateManager().getUpdateStamp(parentSlotKey);
                if (locator.getLastUpdatedTimestamp() > updateStamp.getLastRollupTimestamp()) {
                    rollCount = processLocator(rollCount, executionContext, rollupBatchWriter, locator, true);
                }

            } else {
                rollCount = processLocator(rollCount, executionContext, rollupBatchWriter, locator, isSlotBeingRerolled);
            }
        }
        log.debug("For slotKey {} [isReroll={}], number of locator's that were rolled up are {} out of {}", new Object[]{parentSlotKey, isSlotBeingRerolled, rollCount, locators.size()});

        // now wait until ctx is drained. someone needs to be notified.
        drainExecutionContext(waitStart, rollCount, executionContext, rollupBatchWriter);

        timerCtx.stop();
    }

    protected RollupExecutionContext createRollupExecutionContext() {
        return new RollupExecutionContext(Thread.currentThread());
    }

    protected RollupBatchWriter createRollupBatchWriter(RollupExecutionContext executionContext) {
        return new RollupBatchWriter(rollupWriteExecutor, executionContext);
    }

    public void drainExecutionContext(long waitStart, int rollCount, RollupExecutionContext executionContext, RollupBatchWriter rollupBatchWriter) {
        log.debug("Waiting for rollups to finish for " + parentSlotKey);
        while (!executionContext.doneReading() || !executionContext.doneWriting()) {
            if (executionContext.doneReading()) {
                rollupBatchWriter.drainBatch(); // gets any remaining rollups enqueued for put. should be no-op after being called once
            }
            try {
                waitForRollups();
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

        finishExecution(waitStart, executionContext);
    }

    protected void waitForRollups() throws InterruptedException {
        Thread.currentThread().sleep(LOCATOR_WAIT_FOR_ALL_SECS * 1000);
    }

    public void finishExecution(long waitStart, RollupExecutionContext executionContext) {
        if (executionContext.wasSuccessful()) {
            this.scheduleCtx.clearFromRunning(parentSlotKey);
            log.info("Successful completion of rollups for (gran,slot,shard) {} in {}", new Object[] {parentSlotKey, System.currentTimeMillis() - waitStart});
        } else {
            log.error("Performing BasicRollups for {} failed", parentSlotKey);
            this.scheduleCtx.pushBackToScheduled(parentSlotKey, false);
        }
    }

    public int processLocator(int rollCount, RollupExecutionContext executionContext,
                              RollupBatchWriter rollupBatchWriter, Locator locator, boolean isSlotBeingRerolled) {

        if (isSlotBeingRerolled) {
            Instrumentation.markLocatorReRolled();
        } else {
            Instrumentation.markLocatorRolled();
        }

        return processLocator(rollCount, executionContext, rollupBatchWriter, locator);
    }

    protected int processLocator(int rollCount, RollupExecutionContext executionContext,
                              RollupBatchWriter rollupBatchWriter, Locator locator) {

        if (log.isTraceEnabled())
            log.trace("Rolling up (check,metric,dimension) {} for (gran,slot,shard) {}", locator, parentSlotKey);
        try {
            executeRollupForLocator(executionContext, rollupBatchWriter, locator);
            rollCount += 1;
        } catch (Throwable any) {
            // continue on, but log the problem so that we can fix things later.
            executionContext.markUnsuccessful(any);
            executionContext.decrementReadCounter();
            log.error(any.getMessage(), any);
            log.error("BasicRollup failed for {} at {}", parentSlotKey, serverTime);
        }

        return rollCount;
    }

    public void executeRollupForLocator(RollupExecutionContext executionContext, RollupBatchWriter rollupBatchWriter, Locator locator) {
        executionContext.incrementReadCounter();
        final SingleRollupReadContext singleRollupReadContext = new SingleRollupReadContext(locator, parentRange, getGranularity());
        RollupRunnable rollupRunnable = new RollupRunnable(executionContext, singleRollupReadContext, rollupBatchWriter, enumValidatorExecutor);
        rollupReadExecutor.execute(rollupRunnable);
    }

    public Set<Locator> getLocators(RollupExecutionContext executionContext) {
        Set<Locator> locators = new HashSet<Locator>();

        try {
            // get a list of all locators to rollup for a shard
            locators.addAll(IOContainer.fromConfig().getLocatorIO().getLocators(getShard()));
        } catch (Exception e) {
            executionContext.markUnsuccessful(e);
            log.error("Failed reading locators for slot: " + getParentSlot(), e);
        }
        return locators;
    }
}
