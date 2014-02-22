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
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.exceptions.GranularityException;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.MetricMetadata;
import com.rackspacecloud.blueflood.types.RollupType;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

// Reads prerequisite data for reading a rollup. i.e., RollupType
public class RollupPreReadRunnable implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(RollupPreReadRunnable.class);
    protected final long startWait;
    private final RollupExecutionContext executionContext;
    private final SingleRollupReadContext singleRollupReadContext;
    private final RollupBatchReader batchReader;
    protected static final MetadataCache rollupTypeCache = MetadataCache.createLoadingCacheInstance(
            new TimeValue(48, TimeUnit.HOURS), // todo: need a good default expiration here.
            Configuration.getInstance().getIntegerProperty(CoreConfig.MAX_ROLLUP_READ_THREADS));

    private static final Timer timer = Metrics.timer(RollupService.class, "Rollup Pre-Read Timer");


    public RollupPreReadRunnable(RollupExecutionContext executionContext, SingleRollupReadContext singleRollupReadContext, RollupBatchReader rollupBatchReader) {
        this.singleRollupReadContext = singleRollupReadContext;
        this.executionContext = executionContext;
        this.batchReader = rollupBatchReader;
        startWait = System.currentTimeMillis();
    }

    @Override
    public void run() {
        singleRollupReadContext.getWaitHist().update(System.currentTimeMillis() - startWait);

        Timer.Context ctx = singleRollupReadContext.getExecuteTimerContext();
        Granularity srcGran;
        try {
            srcGran = singleRollupReadContext.getRollupGranularity().finer();
        } catch (GranularityException ex) {
            executionContext.decrementReadCounter();
            ctx.stop();
            return; // no work to be done.
        }

        if (log.isDebugEnabled()) {
            log.debug("Executing rollup pre-read from {} for {} {}", new Object[] {
                    srcGran.shortName(),
                    singleRollupReadContext.getRange().toString(),
                    singleRollupReadContext.getLocator()});
        }

        RollupType rollupType = null;
        Timer.Context timerContext = timer.time();
        try {
            rollupType = RollupType.fromString((String) rollupTypeCache.get(
                    singleRollupReadContext.getLocator(), MetricMetadata.ROLLUP_TYPE.name().toLowerCase()));

            singleRollupReadContext.setRollupType(rollupType);
            batchReader.enqueueRollup(singleRollupReadContext);
        } catch (Throwable th) {
            log.error("Rollup failed; Locator : ", singleRollupReadContext.getLocator()
                    + ", Source Granularity: " + srcGran.name());
            executionContext.decrementReadCounter();
            executionContext.markUnsuccessful(th);
            ctx.stop(); // stop end-to-end timer now since this rollup execution is now effectively over.
        } finally {
            timerContext.stop();
        }
    }
}
