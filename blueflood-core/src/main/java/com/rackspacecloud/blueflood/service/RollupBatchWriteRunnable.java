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
import com.codahale.metrics.Timer;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.utils.Metrics;

import java.util.ArrayList;

public class RollupBatchWriteRunnable  implements Runnable {
    private final RollupExecutionContext executionContext;
    private final ArrayList<SingleRollupWriteContext> writeContexts;
    private static final Histogram rollupsPerBatch = Metrics.histogram(RollupService.class, "Rollups Per Batch");
    private static final Timer batchWriteTimer = Metrics.timer(RollupService.class, "Rollup Batch Write");

    public RollupBatchWriteRunnable(ArrayList<SingleRollupWriteContext> writeContexts, RollupExecutionContext executionContext) {
        this.writeContexts = writeContexts;
        this.executionContext = executionContext;
    }

    @Override
    public void run() {
        Timer.Context ctx = batchWriteTimer.time();
        try {
            AstyanaxWriter.getInstance().insertRollups(writeContexts);
        } catch (ConnectionException e) {
            executionContext.markUnsuccessful(e);
        }
        executionContext.decrementWriteCounter(writeContexts.size());
        rollupsPerBatch.update(writeContexts.size());
        RollupService.lastRollupTime.set(System.currentTimeMillis());
        ctx.stop();
    }
}
