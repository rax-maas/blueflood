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

import com.rackspacecloud.blueflood.io.AstyanaxIO;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.BasicRollup;
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

        log.debug("Executing rollup {}->{} for {} {}", new Object[] {rollupContext.getSourceColumnFamily().getName(),
                rollupContext.getDestinationColumnFamily().getName(), rollupContext.getRange().toString(),
                rollupContext.getLocator()});

        rollupContext.getWaitHist().update(System.currentTimeMillis() - startWait);
        TimerContext timerContext = rollupContext.getExecuteTimer().time();
        try {
            TimerContext calcrollupContext = calcTimer.time();

            // Read data and compute rollup
            BasicRollup rollup;
            try {
                Granularity gran = AstyanaxIO.getCFToGranularityMapper().get(rollupContext.getSourceColumnFamily());
                if (gran == Granularity.FULL) {
                    Points<SimpleNumber> input = AstyanaxReader.getInstance().getSimpleDataToRoll(
                            rollupContext.getLocator(),
                            rollupContext.getRange());
                    rollup = Rollup.BasicFromRaw.compute(input);
                } else {
                    Points<BasicRollup> input = AstyanaxReader.getInstance().getBasicRollupDataToRoll(
                            rollupContext.getLocator(),
                            rollupContext.getRange(),
                            rollupContext.getSourceColumnFamily());
                    rollup = Rollup.BasicFromBasic.compute(input);
                }
            } finally {
                calcrollupContext.stop();
            }

            TimerContext writerollupContext = writeTimer.time();
            try {
                AstyanaxWriter.getInstance().insertRollup(rollupContext.getLocator(),
                        rollupContext.getRange().getStart(), rollup,
                        rollupContext.getDestinationColumnFamily());
            } finally {
                writerollupContext.stop();
            }

            RollupService.lastRollupTime.set(System.currentTimeMillis());
        } catch (Throwable th) {
            log.error("BasicRollup failed; Locator : ", rollupContext.getLocator()
                    + ", Source CF: " + rollupContext.getSourceColumnFamily()
                    + ", Dest CF: " + rollupContext.getDestinationColumnFamily());
        } finally {
            executionContext.decrement();
            timerContext.stop();
        }
    }
}
