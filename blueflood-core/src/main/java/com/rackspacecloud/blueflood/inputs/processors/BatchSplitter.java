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

import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.MetricsCollection;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

public class BatchSplitter extends AsyncFunctionWithThreadPool<MetricsCollection, List<List<Metric>>> {

    private int numPartitions;
    private static int subBatchSize = Configuration.getInstance().getIntegerProperty(CoreConfig.METRIC_SUB_BATCH_SIZE);

    public BatchSplitter(ThreadPoolExecutor threadPool, int numPartitions) {
        super(threadPool);
        setNumPartitions(numPartitions);
    }

    public ListenableFuture<List<List<Metric>>> apply(final MetricsCollection input) throws Exception {
        return getThreadPool().submit(new Callable<List<List<Metric>>>() {
            public List<List<Metric>> call() throws Exception {
                return MetricsCollection.getMetricsAsBatches(input, numPartitions);
            }
        });
    }

    public void setNumPartitions(int i) {
        this.numPartitions = i * subBatchSize;
    }
}
