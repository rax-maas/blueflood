package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.exceptions.GranularityException;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Rollup;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/** rolls up data into one data point, inserts that data point. */
class RollupRunnable implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(RollupRunnable.class);

    private static final Timer calcTimer = Metrics.newTimer(RollupRunnable.class, "Read And Calculate Rollup", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private static final Timer writeTimer = Metrics.newTimer(RollupRunnable.class, "Write Rollup", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private static final Meter rollupHasNoDataMeter = Metrics.newMeter(RollupRunnable.class, "Read and Calculate Rollup Zero Points", "Read Zero Points", TimeUnit.SECONDS);
    private final Locator locator;
    private final RollupContext ctx;
    private final long startWait;
    
    RollupRunnable(RollupContext ctx, Locator locator) {
        this.locator = locator;
        this.ctx = ctx;
        startWait = System.currentTimeMillis();
    }
    
    public void run() {
        if (log.isDebugEnabled()) {
            try {
                log.debug("Executing rollup {}->{} for {} {}", new Object[] {ctx.getSourceGranularity().name(),
                        ctx.getSourceGranularity().coarser().name(), ctx.getRange().toString(), locator});
            } catch (GranularityException ex) {
                log.error("Don't have any granularity coarser than " + ctx.getSourceGranularity());
                return;
            }
        }

        ctx.getWaitHist().update(System.currentTimeMillis() - startWait);
        TimerContext timerContext = ctx.getExecuteTimer().time();
        try {
            TimerContext calcCtx = calcTimer.time();
            Rollup rollup;
            try {
                rollup = AstyanaxReader.getInstance().readAndCalculate(locator, ctx.getRange(),
                        ctx.getSourceGranularity());
                if (rollup.getCount() == 0) { rollupHasNoDataMeter.mark(); }
            } finally {
                calcCtx.stop();
            }

            TimerContext writeCtx = writeTimer.time();
            try {
                AstyanaxWriter.getInstance().insertRollup(locator, ctx.getRange().getStart(), rollup,
                        ctx.getSourceGranularity().coarser());
            } finally {
                writeCtx.stop();
            }

            RollupService.lastRollupTime.set(System.currentTimeMillis());
        } catch (Throwable th) {
            log.error("rollup failed locator: {}", locator);
            log.error("Something unexpected happened", th);  
        } finally {
            ctx.decrement();
            timerContext.stop();
        }
    }
}
