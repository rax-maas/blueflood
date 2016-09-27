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
import com.google.common.collect.Sets;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.exceptions.GranularityException;
import com.rackspacecloud.blueflood.io.AbstractMetricsRW;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.CassandraModel.MetricColumnFamily;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.RollupUtils;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.rackspacecloud.blueflood.eventemitter.RollupEventEmitter;
import com.rackspacecloud.blueflood.eventemitter.RollupEvent;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/** rolls up data into one data point, inserts that data point. */
public class RollupRunnable implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(RollupRunnable.class);

    protected final SingleRollupReadContext singleRollupReadContext;
    protected static final MetadataCache metadataCache = MetadataCache.getInstance();
    protected static final MetadataCache rollupTypeCache = MetadataCache.createLoadingCacheInstance(
            new TimeValue(48, TimeUnit.HOURS), // todo: need a good default expiration here.
            Configuration.getInstance().getIntegerProperty(CoreConfig.MAX_ROLLUP_READ_THREADS));

    protected final RollupExecutionContext executionContext;
    protected final RollupBatchWriter rollupBatchWriter;
    protected final long startWait;

    private static final Timer calcTimer = Metrics.timer(RollupRunnable.class, "Read And Calculate Rollup");
    private static final Meter noPointsToCalculateRollup = Metrics.meter(RollupRunnable.class, "No points to calculate rollup");
    private static HashMap<Granularity, Meter> granToMeters = new HashMap<Granularity, Meter>();
    private ExecutorService enumValidatorExecutor;

    static {
        for (Granularity rollupGranularity : Granularity.rollupGranularities()) {
            granToMeters.put(rollupGranularity, Metrics.meter(RollupRunnable.class, String.format("%s Rollup", rollupGranularity.shortName())));
        }
    }

    public RollupRunnable(RollupExecutionContext executionContext, SingleRollupReadContext singleRollupReadContext, RollupBatchWriter rollupBatchWriter, ExecutorService enumValidatorExecutor) {
        this.executionContext = executionContext;
        this.singleRollupReadContext = singleRollupReadContext;
        this.rollupBatchWriter = rollupBatchWriter;
        this.enumValidatorExecutor = enumValidatorExecutor;
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

        if (LOG.isTraceEnabled()) {
            LOG.trace("Executing rollup from {} for {} {}", new Object[]{
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
            Locator rollupLocator = singleRollupReadContext.getLocator();
            RollupType rollupType = RollupType.fromString((String) rollupTypeCache.get(
                    rollupLocator, MetricMetadata.ROLLUP_TYPE.name().toLowerCase()));

            // RollupType   | Class                          | Column Family
            // -------------| ------------------------------ |--------------
            // COUNTER      | BluefloodCounterRollup         | metrics_preaggr_{gran}
            // TIMER        | BluefloodTimerRollup           | metrics_preaggr_{gran}
            // SET          | BluefloodSetRollup             | metrics_preaggr_{gran}
            // GAUGE        | BluefloodGaugeRollup           | metrics_preaggr_{gran}
            // ENUM         | BluefloodEnumRollup            | metrics_preaggr_{gran}
            // BF_BASIC     | BasicRollup (if gran != full)  | metrics_{gran}
            //              | SimpleNumber (if gran == full) | metrics_full

            Class<? extends Rollup> rollupClass = RollupType.classOf(rollupType, srcGran.coarser());
            MetricColumnFamily srcCF = CassandraModel.getColumnFamily(rollupClass, srcGran);
            Granularity dstGran = srcGran.coarser();
            MetricColumnFamily dstCF = CassandraModel.getColumnFamily(rollupClass, dstGran);

            if (rollupType == RollupType.ENUM) {
                singleRollupReadContext.getEnumMetricsMeterForGranularity(dstGran).mark();
                //Run the validation for enums every 5 minutes, when data is being rolled up from full to 5m
                if (dstGran.equals(Granularity.MIN_5) && Configuration.getInstance().getBooleanProperty(CoreConfig.ENUM_VALIDATOR_ENABLED) == true) {
                    enumValidatorExecutor.execute(new EnumValidator(Sets.newHashSet(rollupLocator)));
                }
            }

            // first, get the points.
            AbstractMetricsRW metricsRW;
            try {
                metricsRW = RollupUtils.getMetricsRWForRollupType(rollupType);

                input = metricsRW.getDataToRollup(
                        singleRollupReadContext.getLocator(),
                        rollupType,
                        singleRollupReadContext.getRange(),
                        srcCF.getName());

                if (input.isEmpty()) {
                    LOG.debug(String.format("No points rollup for locator %s", singleRollupReadContext.getLocator()));
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
            //Emit a rollup event to event emitter
            RollupEventEmitter.getInstance().emit(RollupEventEmitter.ROLLUP_EVENT_NAME,
                    new RollupEvent(singleRollupReadContext.getLocator(), rollup,
                            metadataCache.getUnitString(singleRollupReadContext.getLocator()),
                            singleRollupReadContext.getRollupGranularity().name(),
                            singleRollupReadContext.getRange().getStart()));
        } catch (Exception e) {
            LOG.error("Rollup failed; Locator: {}, Source Granularity: {}, For period: {}", new Object[]{
                    singleRollupReadContext.getLocator(),
                    srcGran.name(),
                    singleRollupReadContext.getRange().toString(),
                    e});
        } finally {
            executionContext.decrementReadCounter();
            timerContext.stop();
        }
    }

    // determine which DataType to use for serialization.
    public static Rollup.Type getRollupComputer(RollupType srcType, Granularity srcGran) {
        switch (srcType) {
            case COUNTER:
                return Rollup.CounterFromCounter;
            case TIMER:
                return Rollup.TimerFromTimer;
            case GAUGE:
                return Rollup.GaugeFromGauge;
            case BF_BASIC:
                return srcGran == Granularity.FULL ? Rollup.BasicFromRaw : Rollup.BasicFromBasic;
            case SET:
                return Rollup.SetFromSet;
            case ENUM:
                return Rollup.EnumFromEnum;
            default:
                break;
        }
        throw new IllegalArgumentException(String.format("Cannot compute rollups for %s from %s", srcType.name(), srcGran.shortName()));
    }
}
