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
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.io.AstyanaxIO;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.types.CounterRollup;
import com.rackspacecloud.blueflood.types.GaugeRollup;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Points;
import com.rackspacecloud.blueflood.types.Rollup;
import com.rackspacecloud.blueflood.types.SetRollup;
import com.rackspacecloud.blueflood.types.SimpleNumber;
import com.rackspacecloud.blueflood.types.StatType;
import com.rackspacecloud.blueflood.types.TimerRollup;
import com.rackspacecloud.blueflood.utils.TimeValue;
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
    private static final MetadataCache rollupTypeCache = MetadataCache.createLoadingCacheInstance(new TimeValue(48, TimeUnit.HOURS), 20); // todo: need a good default here.
    
    private final RollupContext rollupContext;
    private final RollupExecutionContext executionContext;
    private final long startWait;
    
    RollupRunnable(RollupExecutionContext executionContext, RollupContext rollupContext) {
        this.executionContext = executionContext;
        this.rollupContext = rollupContext;
        startWait = System.currentTimeMillis();
    }
    
    public void run() {
        // done waiting.
        rollupContext.getWaitHist().update(System.currentTimeMillis() - startWait);
        
        if (log.isDebugEnabled()) {
            log.debug("Executing rollup from {} for {} {}", new Object[] {
                    rollupContext.getSourceGranularity().shortName(),
                    rollupContext.getRange().toString(),
                    rollupContext.getLocator()});
        }
        
        // start timing this action.
        TimerContext timerContext = rollupContext.getExecuteTimer().time();
        try {
            TimerContext calcrollupContext = calcTimer.time();

            // Read data and compute rollup
            Points input;
            Rollup rollup = null;
            ColumnFamily<Locator, Long> srcCF;
            ColumnFamily<Locator, Long> dstCF = AstyanaxIO.getColumnFamilyMapper().get(rollupContext.getSourceGranularity().coarser().name());;
            StatType statType = StatType.fromString(rollupTypeCache.get(rollupContext.getLocator(), StatType.CACHE_KEY).toString());
            Class<? extends Rollup> rollupClass = RollupRunnable.classOf(statType, rollupContext.getSourceGranularity());
            
            try {
                // first, get the points.
                if (rollupContext.getSourceGranularity() == Granularity.FULL) {    
                    srcCF = statType == StatType.UNKNOWN
                            ? AstyanaxIO.CF_METRICS_FULL
                            : AstyanaxIO.CF_METRICS_PREAGGREGATED;
                    input = AstyanaxReader.getInstance().getDataToRoll(rollupClass,
                            rollupContext.getLocator(), rollupContext.getRange(), srcCF);
                } else {
                    srcCF = AstyanaxIO.getColumnFamilyMapper().get(rollupContext.getSourceGranularity().name());
                    input = AstyanaxReader.getInstance().getBasicRollupDataToRoll(
                            rollupContext.getLocator(),
                            rollupContext.getRange(),
                            srcCF);
                }
                
                // next, compute the rollup.
                rollup =  RollupRunnable.getRollupComputer(statType, rollupContext.getSourceGranularity()).compute(input);
                
            } catch (IllegalArgumentException ex) {
                // todo: invalid types. log and get out.
            } finally {
                calcrollupContext.stop();
            }

            // now save the new rollup.
            TimerContext writerollupContext = writeTimer.time();
            try {
                AstyanaxWriter.getInstance().insertRollup(
                        rollupContext.getLocator(),
                        rollupContext.getRange().getStart(),
                        rollup,
                        dstCF);
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
    
    // derive the class of the type. This will be used to determine which serializer is used.
    public static Class<? extends Rollup> classOf(StatType type, Granularity gran) {
        if (type == StatType.COUNTER)
            return CounterRollup.class;
        else if (type == StatType.TIMER)
            return TimerRollup.class;
        else if (type == StatType.SET)
            return SetRollup.class;
        else if (type == StatType.GAUGE)
            return GaugeRollup.class;
        else if (type == StatType.UNKNOWN && gran == Granularity.FULL)
            return SimpleNumber.class;
        else if (type == StatType.UNKNOWN && gran != Granularity.FULL)
            return BasicRollup.class;
        else
            throw new IllegalArgumentException(String.format("Unexpected type/gran combination: %s, %s", type, gran));
    }
    
    // dertmine which Type to use for serialization.
    // dertmine which Type to use for serialization.
    public static Rollup.Type getRollupComputer(StatType srcType, Granularity srcGran) {
        switch (srcType) {
            case COUNTER:
                return Rollup.CounterFromCounter;
            case TIMER:
                return Rollup.TimerFromTimer;
            case GAUGE:
                return Rollup.GaugeFromGauge;
            case UNKNOWN:
                return srcGran == Granularity.FULL ? Rollup.BasicFromRaw : Rollup.BasicFromBasic;
            case SET:
            default:
                break;
        }
        throw new IllegalArgumentException(String.format("Cannot compute rollups for %s from %s", srcType.name(), srcGran.shortName()));
    }
}
