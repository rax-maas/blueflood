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
import com.rackspacecloud.blueflood.exceptions.GranularityException;
import com.rackspacecloud.blueflood.io.AstyanaxIO;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
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

    protected final SingleRollupReadContext singleRollupReadContext;
    protected static final MetadataCache rollupTypeCache = MetadataCache.createLoadingCacheInstance(
            new TimeValue(48, TimeUnit.HOURS), // todo: need a good default expiration here.
            Configuration.getInstance().getIntegerProperty(CoreConfig.MAX_ROLLUP_READ_THREADS));

    protected final RollupExecutionContext executionContext;
    protected final RollupBatchWriter rollupBatchWriter;
    protected final long startWait;

    private static final Timer calcTimer = Metrics.newTimer(RollupRunnable.class, "Read And Calculate Rollup", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

    
    RollupRunnable(RollupExecutionContext executionContext, SingleRollupReadContext singleRollupReadContext, RollupBatchWriter rollupBatchWriter) {
        this.executionContext = executionContext;
        this.singleRollupReadContext = singleRollupReadContext;
        this.rollupBatchWriter = rollupBatchWriter;
        startWait = System.currentTimeMillis();
    }
    
    public void run() {
        // done waiting.
        singleRollupReadContext.getWaitHist().update(System.currentTimeMillis() - startWait);

        Granularity srcGran;
        try {
            srcGran = singleRollupReadContext.getRollupGranularity().finer();
        } catch (GranularityException ex) {
            executionContext.decrementReadCounter();
            return; // no work to be done.
        }


        if (log.isDebugEnabled()) {
            log.debug("Executing rollup from {} for {} {}", new Object[] {
                    srcGran.shortName(),
                    singleRollupReadContext.getRange().toString(),
                    singleRollupReadContext.getLocator()});
        }

        // start timing this action.
        TimerContext timerContext = singleRollupReadContext.getExecuteTimer().time();

        try {
            TimerContext calcrollupContext = calcTimer.time();

            // Read data and compute rollup
            Points input;
            Rollup rollup = null;
            ColumnFamily<Locator, Long> srcCF;
            ColumnFamily<Locator, Long> dstCF;
            StatType statType = StatType.fromString((String)rollupTypeCache.get(singleRollupReadContext.getLocator(), StatType.CACHE_KEY));
            Class<? extends Rollup> rollupClass = RollupRunnable.classOf(statType, srcGran);
            
            // short circuit for sets, which are not rolled up, since we don't want to waste time reading them from
            // full res.
            if (statType == StatType.SET)
                return;
            
            try {
                // first, get the points.
                if (srcGran == Granularity.FULL) {
                    srcCF = statType == StatType.UNKNOWN
                            ? AstyanaxIO.CF_METRICS_FULL
                            : AstyanaxIO.CF_METRICS_PREAGGREGATED_FULL;
                    input = AstyanaxReader.getInstance().getDataToRoll(rollupClass,
                            singleRollupReadContext.getLocator(), singleRollupReadContext.getRange(), srcCF);
                } else {
                    srcCF = AstyanaxIO.getColumnFamilyMapper().get(srcGran);
                    input = AstyanaxReader.getInstance().getBasicRollupDataToRoll(
                            singleRollupReadContext.getLocator(),
                            singleRollupReadContext.getRange(),
                            srcCF);
                }
                
                dstCF = statType == StatType.UNKNOWN
                        ? AstyanaxIO.getColumnFamilyMapper().get(srcGran.coarser())
                        : AstyanaxIO.getPreagColumnFamilyMapper().get(srcGran.coarser());
                
                // next, compute the rollup.
                rollup =  RollupRunnable.getRollupComputer(statType, srcGran).compute(input);
                
            } finally {
                calcrollupContext.stop();
            }
            // now enqueue the new rollup for writing.
            rollupBatchWriter.enqueueRollupForWrite(new SingleRollupWriteContext(rollup, singleRollupReadContext, dstCF));

            RollupService.lastRollupTime.set(System.currentTimeMillis());
        } catch (Throwable th) {
            log.error("Rollup failed; Locator : ", singleRollupReadContext.getLocator()
                    + ", Source Granularity: " + srcGran.name());
        } finally {
            executionContext.decrementReadCounter();
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
    public static Rollup.Type getRollupComputer(StatType srcType, Granularity srcGran) {
        switch (srcType) {
            case COUNTER:
                return Rollup.CounterFromCounter;
            case TIMER:
                return Rollup.TimerFromTimer;
            case GAUGE:
                return Rollup.GaugeFromGauge;
            case BF_HISTOGRAMS:
                return srcGran == Granularity.FULL ? Rollup.HistogramFromRaw : Rollup.HistogramFromHistogram;
            case UNKNOWN:
                return srcGran == Granularity.FULL ? Rollup.BasicFromRaw : Rollup.BasicFromBasic;
            case SET:
                // we do not roll sets up.
            default:
                break;
        }
        throw new IllegalArgumentException(String.format("Cannot compute rollups for %s from %s", srcType.name(), srcGran.shortName()));
    }
}
