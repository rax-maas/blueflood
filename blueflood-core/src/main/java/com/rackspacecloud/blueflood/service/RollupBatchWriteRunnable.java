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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.rackspacecloud.blueflood.io.AbstractMetricsRW;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RollupBatchWriteRunnable implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(RollupBatchWriteRunnable.class);
    private static final Histogram rollupsPerBatch = Metrics.histogram(RollupService.class, "Rollups Per Batch");
    private static final Meter rollupsWriteRate = Metrics.meter(RollupService.class, "Rollups Write Rate");
    private static final Timer batchWriteTimer = Metrics.timer(RollupService.class, "Rollup Batch Write");

    private final RollupExecutionContext executionContext;
    private final List<SingleRollupWriteContext> writeContexts;
    private final AbstractMetricsRW metricsRW;

    public RollupBatchWriteRunnable(List<SingleRollupWriteContext> writeContexts,
                                    RollupExecutionContext executionContext,
                                    AbstractMetricsRW metricsRW) {
        this.writeContexts = writeContexts;
        this.executionContext = executionContext;
        this.metricsRW = metricsRW;
    }

    @Override
    public void run() {
        Timer.Context ctx = batchWriteTimer.time();
        try {
            metricsRW.insertRollups(writeContexts);
        } catch (Exception e) {
            LOG.warn("not able to insert rollups", e);
            executionContext.markUnsuccessful(e);
        } finally {
            executionContext.decrementWriteCounter(writeContexts.size());
            rollupsPerBatch.update(writeContexts.size());
            rollupsWriteRate.mark(writeContexts.size());
            RollupService.lastRollupTime.set(System.currentTimeMillis());
            ctx.stop();
        }
    }
}
