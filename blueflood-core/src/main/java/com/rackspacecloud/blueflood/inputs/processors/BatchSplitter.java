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

package com.rackspacecloud.blueflood.inputs.processors;

import com.codahale.metrics.Timer;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;

import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.MetricsCollection;
import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.utils.Metrics;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

public class BatchSplitter extends AsyncFunctionWithThreadPool<MetricsCollection, List<List<IMetric>>> {
    private int batchSize;
    private final Timer splitDurationTimer = Metrics.timer(BatchSplitter.class, "Split Duration");

    public BatchSplitter(ThreadPoolExecutor threadPool, int batchSize) {
        super(threadPool);
        this.batchSize = batchSize;
    }

    public ListenableFuture<List<List<IMetric>>> apply(final MetricsCollection input) throws Exception {
        return getThreadPool().submit(new Callable<List<List<IMetric>>>() {
            final Timer.Context actualSplitContext = splitDurationTimer.time();

            public List<List<IMetric>> call() throws Exception {
                try {
                    return input.splitMetricsIntoBatches(batchSize);
                }
                finally {
                    actualSplitContext.stop();
                }
            }
        });
    }
}
