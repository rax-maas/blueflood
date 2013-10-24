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
import com.rackspacecloud.blueflood.io.AstyanaxIO;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Points;
import com.rackspacecloud.blueflood.types.Rollup;
import com.rackspacecloud.blueflood.types.SimpleNumber;
import com.yammer.metrics.Metrics;
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
    private final RollupContext rollupContext;
    private final RollupExecutionContext executionContext;
    private final long startWait;
    
    RollupRunnable(RollupExecutionContext executionContext, RollupContext rollupContext) {
        this.executionContext = executionContext;
        this.rollupContext = rollupContext;
        startWait = System.currentTimeMillis();
    }
    
    public void run() {
        if (log.isDebugEnabled()) {
            log.debug("Executing rollup from {} for {} {}", new Object[] {
                    rollupContext.getSourceGranularity().shortName(),
                    rollupContext.getRange().toString(),
                    rollupContext.getLocator()});
        }

        rollupContext.getWaitHist().update(System.currentTimeMillis() - startWait);
        TimerContext timerContext = rollupContext.getExecuteTimer().time();
        try {
            TimerContext calcrollupContext = calcTimer.time();
            final ColumnFamily<Locator, Long> srcCF = AstyanaxIO.getColumnFamilyMapper().get(
                    rollupContext.getSourceGranularity().name());
            final ColumnFamily<Locator, Long> destCF = AstyanaxIO.getColumnFamilyMapper().get(
                    rollupContext.getSourceGranularity().coarser().name());

            // Read data and compute rollup
            Rollup rollup;
            try {
                if (rollupContext.getSourceGranularity() == Granularity.FULL) {
                    Points<SimpleNumber> input = AstyanaxReader.getInstance().getSimpleDataToRoll(
                            rollupContext.getLocator(),
                            rollupContext.getRange());
                    rollup = Rollup.BasicFromRaw.compute(input);
                } else {
                    Points<BasicRollup> input = AstyanaxReader.getInstance().getBasicRollupDataToRoll(
                            rollupContext.getLocator(),
                            rollupContext.getRange(),
                            srcCF);
                    rollup = Rollup.BasicFromBasic.compute(input);
                }
            } finally {
                calcrollupContext.stop();
            }

            TimerContext writerollupContext = writeTimer.time();
            try {
                AstyanaxWriter.getInstance().insertRollup(
                        rollupContext.getLocator(),
                        rollupContext.getRange().getStart(),
                        rollup,
                        destCF);
            } finally {
                writerollupContext.stop();
            }

            RollupService.lastRollupTime.set(System.currentTimeMillis());
        } catch (Throwable th) {
            log.error("Rollup failed; Locator : ", rollupContext.getLocator() 
                    + ", Source Granularity: " + rollupContext.getSourceGranularity().name());
        } finally {
            executionContext.decrement();
            timerContext.stop();
        }
    }
}
