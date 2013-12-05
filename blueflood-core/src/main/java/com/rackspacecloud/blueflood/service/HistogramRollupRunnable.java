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

import com.netflix.astyanax.model.ColumnFamily;
import com.rackspacecloud.blueflood.exceptions.GranularityException;
import com.rackspacecloud.blueflood.io.AstyanaxIO;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class HistogramRollupRunnable extends RollupRunnable {
    private static final Logger log = LoggerFactory.getLogger(HistogramRollupRunnable.class);

    private static final Timer calcTimer = Metrics.newTimer(RollupRunnable.class, "Read And Calculate Histogram",
            TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

    public HistogramRollupRunnable(RollupExecutionContext executionContext,
                                   SingleRollupReadContext singleRollupReadContext,
                                   RollupBatchWriter rollupBatchWriter) {
        super(executionContext, singleRollupReadContext, rollupBatchWriter);
    }

    public void run() {
        singleRollupReadContext.getWaitHist().update(System.currentTimeMillis() - startWait);
        Granularity dstGran = singleRollupReadContext.getRollupGranularity();
        Granularity srcGran;
        try {
            singleRollupReadContext.getRollupGranularity().finer();
        } catch (GranularityException ex) {
            executionContext.decrementReadCounter();
            return; // no work to be done.
        }

        if (dstGran.isCoarser(Granularity.MIN_5)) {
            srcGran = Granularity.MIN_5;
        } else {
            srcGran = Granularity.FULL;
        }

        if (log.isDebugEnabled()) {
            log.debug("Executing histogram rollup from {} for {} {}", new Object[] {
                    srcGran.shortName(),
                    singleRollupReadContext.getRange().toString(),
                    singleRollupReadContext.getLocator()});
        }

        TimerContext timerContext = singleRollupReadContext.getExecuteTimer().time();
        try {
            // Read data and compute rollup
            Points<HistogramRollup> input;
            Rollup rollup = null;
            ColumnFamily<Locator, Long> srcCF;
            ColumnFamily<Locator, Long> dstCF = AstyanaxIO.getHistogramColumnFamilyMapper().get(dstGran);
            StatType statType = StatType.fromString((String) rollupTypeCache.get(singleRollupReadContext.getLocator(),
                    StatType.CACHE_KEY));

            if (statType != StatType.UNKNOWN) { // Do not compute histogram for statsd metrics.
                executionContext.decrementReadCounter();
                timerContext.stop();
                return;
            }

            if (srcGran == Granularity.MIN_5) {
                srcCF = AstyanaxIO.CF_METRICS_FULL;
            } else {
                // Coarser histograms are always computed from 5 MIN histograms for error minimization
                srcCF = AstyanaxIO.CF_METRICS_HIST_5M;
            }

            TimerContext calcrollupContext = calcTimer.time();
            try {
                input = AstyanaxReader.getInstance().getDataToRoll(
                            HistogramRollup.class,
                            singleRollupReadContext.getLocator(),
                            singleRollupReadContext.getRange(),
                            srcCF);

                // next, compute the rollup.
                rollup =  RollupRunnable.getRollupComputer(StatType.BF_HISTOGRAMS, srcGran).compute(input);
            } finally {
                calcrollupContext.stop();
            }

            if (rollup != null) {
                rollupBatchWriter.enqueueRollupForWrite(new SingleRollupWriteContext(rollup, singleRollupReadContext, dstCF));
            }
            RollupService.lastRollupTime.set(System.currentTimeMillis());
        } catch (Throwable th) {
            log.error("Histogram rollup failed; Locator : ", singleRollupReadContext.getLocator()
                    + ", Source Granularity: " + srcGran.name());
        } finally {
            executionContext.decrementReadCounter();
            timerContext.stop();
        }
    }
}
