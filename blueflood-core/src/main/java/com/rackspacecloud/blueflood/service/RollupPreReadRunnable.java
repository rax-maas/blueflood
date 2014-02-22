/*
 * Copyright 2014 Rackspace
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
import com.netflix.astyanax.model.ColumnFamily;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.exceptions.GranularityException;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

// Reads prerequisite data for reading a rollup. i.e., RollupType
public class RollupPreReadRunnable implements Runnable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(RollupPreReadRunnable.class);
    protected final long startWait;
    private final RollupExecutionContext executionContext;
    private final SingleRollupReadContext singleRollupReadContext;
    protected static final MetadataCache rollupTypeCache = MetadataCache.createLoadingCacheInstance(
            new TimeValue(48, TimeUnit.HOURS), // todo: need a good default expiration here.
            Configuration.getInstance().getIntegerProperty(CoreConfig.MAX_ROLLUP_READ_THREADS));

    private static final Timer timer = Metrics.timer(RollupService.class, "Rollup Pre-Read Timer");

    public RollupPreReadRunnable(RollupExecutionContext executionContext, SingleRollupReadContext singleRollupReadContext, RollupBatchReader rollupBatchReader) {
        this.singleRollupReadContext = singleRollupReadContext;
        this.executionContext = executionContext;
        startWait = System.currentTimeMillis();
    }

    @Override
    public void run() {
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

        // Read data and compute rollup
        Rollup rollup = null;
        RollupType rollupType = null;
        Timer.Context timerContext = timer.time();
        try {
            rollupType = RollupType.fromString((String) rollupTypeCache.get(
                    singleRollupReadContext.getLocator(), MetricMetadata.ROLLUP_TYPE.name().toLowerCase()));

            Class<? extends Rollup> rollupClass = RollupType.classOf(rollupType, srcGran.coarser());
            ColumnFamily<Locator, Long> srcCF = CassandraModel.getColumnFamily(rollupClass, srcGran);
            ColumnFamily<Locator, Long> dstCF = CassandraModel.getColumnFamily(rollupClass, srcGran.coarser());
        } catch (Throwable th) {
            log.error("Rollup failed; Locator : ", singleRollupReadContext.getLocator()
                    + ", Source Granularity: " + srcGran.name());
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
