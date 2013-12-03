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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadPoolExecutor;

// Batches rollup writes
public class RollupBatchWriter {
    private final Logger log = LoggerFactory.getLogger(RollupBatchWriter.class);
    private final ThreadPoolExecutor executor;
    private final RollupExecutionContext context;
    private final ConcurrentLinkedQueue<SingleRollupWriteContext> rollupQueue = new ConcurrentLinkedQueue<SingleRollupWriteContext>();
    private static final int ROLLUP_BATCH_MIN_SIZE = Configuration.getInstance().getIntegerProperty(CoreConfig.ROLLUP_BATCH_MIN_SIZE);
    private static final int ROLLUP_BATCH_MAX_SIZE = Configuration.getInstance().getIntegerProperty(CoreConfig.ROLLUP_BATCH_MAX_SIZE);

    public RollupBatchWriter(ThreadPoolExecutor executor, RollupExecutionContext context) {
        this.executor = executor;
        this.context = context;
    }


    public void enqueueRollupForWrite(SingleRollupWriteContext rollupWriteContext) {
        rollupQueue.add(rollupWriteContext);
        context.incrementWriteCounter();
        // enqueue MIN_SIZE batches only if the threadpool is unsaturated. else, enqueue when we have >= MAX_SIZE pending
        if (rollupQueue.size() >= ROLLUP_BATCH_MIN_SIZE) {
            if (executor.getActiveCount() < executor.getPoolSize() || rollupQueue.size() >= ROLLUP_BATCH_MAX_SIZE) {
                drainBatch();
            }
        }
    }

    public synchronized void drainBatch() {
        ArrayList<SingleRollupWriteContext> writeContexts = new ArrayList<SingleRollupWriteContext>();
        SingleRollupWriteContext ctx;
        try {
            for (int i=0; i<=ROLLUP_BATCH_MAX_SIZE; i++) {
                writeContexts.add(rollupQueue.remove());
            }
        } catch (NoSuchElementException e) {
            // pass
        }
        if (writeContexts.size() > 0) {
            executor.execute(new RollupBatchWriteRunnable(writeContexts, context));
        }
    }
}
