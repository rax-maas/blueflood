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

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.netflix.astyanax.model.ColumnFamily;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.exceptions.GranularityException;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.rackspacecloud.blueflood.eventemitter.RollupEventEmitter;
import com.rackspacecloud.blueflood.eventemitter.RollupEvent;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/** rolls up data into one data point, inserts that data point. */
public class RollupRunnable implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(RollupRunnable.class);

    private static final Timer writeTimer = Metrics.timer(RollupRunnable.class, "Write Rollup");
    protected final SingleRollupReadContext singleRollupReadContext;
    protected static final MetadataCache rollupTypeCache = MetadataCache.createLoadingCacheInstance(
            new TimeValue(48, TimeUnit.HOURS), // todo: need a good default expiration here.
            Configuration.getInstance().getIntegerProperty(CoreConfig.MAX_ROLLUP_READ_THREADS));

    protected final RollupExecutionContext executionContext;
    protected final RollupBatchWriter rollupBatchWriter;
    protected final long startWait;

    private static final Timer calcTimer = Metrics.timer(RollupRunnable.class, "Read And Calculate Rollup");
    private static final Meter noPointsToCalculateRollup = Metrics.meter(RollupRunnable.class, "No points to calculate rollup");
    private static HashMap<Granularity, Meter> granToMeters = new HashMap<Granularity, Meter>();

    static {
        for (Granularity rollupGranularity : Granularity.rollupGranularities()) {
            granToMeters.put(rollupGranularity, Metrics.meter(RollupRunnable.class, String.format("%s Rollup", rollupGranularity.shortName())));
        }
    }

    public RollupRunnable(RollupExecutionContext executionContext, SingleRollupReadContext singleRollupReadContext, RollupBatchWriter rollupBatchWriter) {
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
            log.trace("Executing rollup from {} for {} {}", new Object[] {
                    srcGran.shortName(),
                    singleRollupReadContext.getRange().toString(),
                    singleRollupReadContext.getLocator()});
        }

        // start timing this action.
        Timer.Context timerContext = singleRollupReadContext.getExecuteTimer().time();

        try {
            Timer.Context calcrollupContext = calcTimer.time();
            granToMeters.get(srcGran.coarser()).mark();

            // Read data and compute rollup
            Points input;
            Rollup rollup = null;
            RollupType rollupType = RollupType.fromString((String) rollupTypeCache.get(
                    singleRollupReadContext.getLocator(), MetricMetadata.ROLLUP_TYPE.name().toLowerCase()));
            Class<? extends Rollup> rollupClass = RollupType.classOf(rollupType, srcGran.coarser());
            ColumnFamily<Locator, Long> srcCF = CassandraModel.getColumnFamily(rollupClass, srcGran);
            ColumnFamily<Locator, Long> dstCF = CassandraModel.getColumnFamily(rollupClass, srcGran.coarser());

            try {
                // first, get the points.
                input = AstyanaxReader.getInstance().getDataToRoll(rollupClass,
                        singleRollupReadContext.getLocator(), singleRollupReadContext.getRange(), srcCF);

                if (input.isEmpty()) {
                    noPointsToCalculateRollup.mark();
                    return;
                }

                // next, compute the rollup.
                rollup =  RollupRunnable.getRollupComputer(rollupType, srcGran).compute(input);
            } finally {
                calcrollupContext.stop();
            }
            // now enqueue the new rollup for writing.
            rollupBatchWriter.enqueueRollupForWrite(new SingleRollupWriteContext(rollup, singleRollupReadContext, dstCF));

            RollupService.lastRollupTime.set(System.currentTimeMillis());
            //Emit a rollup event to eventemitter
            RollupEventEmitter.getInstance().emit(RollupEventEmitter.ROLLUP_EVENT_NAME,
                    new RollupEvent(singleRollupReadContext.getLocator(), rollup,
                            AstyanaxReader.getUnitString(singleRollupReadContext.getLocator()),
                            singleRollupReadContext.getRollupGranularity().name(),
                            singleRollupReadContext.getRange().getStart()));
        } catch (Exception e) {
            log.error("Rollup failed; Locator: {}, Source Granularity: {}, For period: {}", new Object[] {
                    singleRollupReadContext.getLocator(),
                    singleRollupReadContext.getRange().toString(),
                    srcGran.name(),
                    e});
        } finally {
            executionContext.decrementReadCounter();
            timerContext.stop();
        }
    }

    // dertmine which DataType to use for serialization.
    public static Rollup.Type getRollupComputer(RollupType srcType, Granularity srcGran) {
        switch (srcType) {
            case COUNTER:
                return Rollup.CounterFromCounter;
            case TIMER:
                return Rollup.TimerFromTimer;
            case GAUGE:
                return Rollup.GaugeFromGauge;
            case BF_HISTOGRAMS:
                return srcGran == Granularity.FULL ? Rollup.HistogramFromRaw : Rollup.HistogramFromHistogram;
            case BF_BASIC:
                return srcGran == Granularity.FULL ? Rollup.BasicFromRaw : Rollup.BasicFromBasic;
            case SET:
                return Rollup.SetFromSet;
            default:
                break;
        }
        throw new IllegalArgumentException(String.format("Cannot compute rollups for %s from %s", srcType.name(), srcGran.shortName()));
    }
}
