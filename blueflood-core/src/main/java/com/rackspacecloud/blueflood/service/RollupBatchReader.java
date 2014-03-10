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

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multiset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

public class RollupBatchReader {
    private static final Logger log = LoggerFactory.getLogger(RollupBatchReader.class);

    private final ThreadPoolExecutor executor;
    private final RollupExecutionContext context;
    private final RollupBatchWriter writer;
    private final LinkedListMultimap<String, SingleRollupReadContext> rollupQueues = LinkedListMultimap.create();
    private static final int ROLLUP_BATCH_MIN_SIZE = Configuration.getInstance().getIntegerProperty(CoreConfig.ROLLUP_BATCH_MIN_SIZE);
    private static final int ROLLUP_BATCH_MAX_SIZE = Configuration.getInstance().getIntegerProperty(CoreConfig.ROLLUP_BATCH_MAX_SIZE);

    public RollupBatchReader(ThreadPoolExecutor executor, RollupExecutionContext context, RollupBatchWriter writer) {
        this.executor = executor;
        this.context = context;
        this.writer = writer;
    }


    public synchronized void enqueueRollup(SingleRollupReadContext rollupReadContext) {
        String key = rollupReadContext.getBatchGroupingIdentifier();
        rollupQueues.put(key, rollupReadContext);

        // enqueue MIN_SIZE batches only if the threadpool is unsaturated. else, enqueue when we have >= MAX_SIZE pending
        if (rollupQueues.get(key).size() >= ROLLUP_BATCH_MIN_SIZE) {
            if (executor.getActiveCount() < executor.getPoolSize() || rollupQueues.get(key).size() >= ROLLUP_BATCH_MAX_SIZE) {
                drainBatch();
            }
        }
    }

    public synchronized void drainBatch(String queueKey) {
        List<SingleRollupReadContext> queue = rollupQueues.get(queueKey);

        ArrayList<SingleRollupReadContext> readContexts = new ArrayList<SingleRollupReadContext>();
        try {
            for (int i=0; i<=ROLLUP_BATCH_MAX_SIZE; i++) {
                readContexts.add(queue.remove(0));
            }
        } catch (IndexOutOfBoundsException e) {
            // pass
        }
        if (readContexts.size() > 0) {
            executor.execute(new RollupBatchReadRunnable(readContexts, context, writer));
        }
    }

    private String getLargestQueueKey() {
        String biggest = null;
        Integer biggestSize = 0;
        Multiset<String> keys = rollupQueues.keys();
        for (String key : keys.elementSet()) {
            if (keys.count(key) > biggestSize) {
                biggestSize = keys.count(key);
                biggest = key;
            }
        }

        return biggest;
    }

    public void drainBatches() {
        for (String queueKey : rollupQueues.keySet()) {
            drainBatch(queueKey);
        }
    }

    public void drainBatch() {
        String queueKey = getLargestQueueKey();
        drainBatch(queueKey);
    }
}
