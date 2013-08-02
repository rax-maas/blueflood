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
        final Granularity parentGran = Granularity.granularityFromKey(parentSlotKey);
        final int parentSlot = Granularity.slotFromKey(parentSlotKey);
        final int shard = Granularity.shardFromKey(parentSlotKey);
        final Range parentRange = parentGran.deriveRange(parentSlot, serverTime);
        Granularity finerGran;

        try {
            finerGran = parentGran.finer();
        } catch (Exception ex) {
            log.error("No finer granularity available than " + parentGran);
            return;
        }
        final RollupContext ctx = new RollupContext(parentRange, finerGran, Thread.currentThread());

        if (log.isTraceEnabled())
            log.trace("Getting locators for {} {} @ {}", new Object[]{parentSlotKey, parentRange.toString(), scheduleCtx.getCurrentTimeMillis()});
        // todo: I can see this set becoming a memory hog.  There might be a better way of doing this.
        boolean success = true;
        long waitStart = System.currentTimeMillis();
        int rollCount = 0;

        for (Column<Locator> locatorCol : AstyanaxReader.getInstance().getAllLocators(shard)) {
            if (log.isTraceEnabled())
                log.trace("Rolling up (check,metric,dimension) {} for (gran,slot,shard) {}", locatorCol.getName(), parentSlotKey);
            try {
                ctx.increment();
                rollupExecutor.execute(new RollupRunnable(ctx, locatorCol.getName()));
                rollCount += 1;
            } catch (Throwable any) {
                // continue on, but log the problem so that we can fix things later.
                ctx.decrement();
                log.error(any.getMessage(), any);
                log.error("Rollup failed for {} at {}", parentSlotKey, serverTime);
            }
        }
        
        if (success) {
            // now wait until ctx is drained. someone needs to be notified.
            log.debug("Waiting for rollups to finish for " + parentSlotKey);
            while (!ctx.done()) {
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
            log.warn("Rollup execution of {} failed.", parentGran);
            this.scheduleCtx.pushBackToScheduled(parentSlotKey);
        }

        timerCtx.stop();
    }
}
