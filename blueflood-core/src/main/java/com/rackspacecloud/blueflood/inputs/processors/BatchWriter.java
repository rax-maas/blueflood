/*
 * Copyright 2013-2015 Rackspace
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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.rackspacecloud.blueflood.concurrent.FunctionWithThreadPool;
import com.rackspacecloud.blueflood.io.IMetricsWriter;
import com.rackspacecloud.blueflood.service.IngestionContext;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.PreaggregatedMetric;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.TimeValue;
import com.rackspacecloud.blueflood.utils.Util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

public class BatchWriter extends FunctionWithThreadPool<List<List<IMetric>>, ListenableFuture<List<Boolean>>> {
        
    private final BatchIdGenerator batchIdGenerator = new BatchIdGenerator();
    // todo: CM_SPECIFIC verify changing metric class name doesn't break things.
    private final Timer writeDurationTimer = Metrics.timer(BatchWriter.class, "Write Duration");
    private final Timer batchWriteDurationTimer = Metrics.timer(BatchWriter.class, "Single Batch Write Duration");
    private final Timer slotUpdateTimer = Metrics.timer(BatchWriter.class, "Slot Update Duration");
    private final Meter exceededScribeProcessingTime = Metrics.meter(BatchWriter.class, "Write Duration Exceeded Timeout");
    private final TimeValue timeout;
    private final Counter bufferedMetrics;
    private final IngestionContext context;
    
    private final IMetricsWriter writer;
    
    public BatchWriter(ThreadPoolExecutor threadPool, IMetricsWriter writer, TimeValue timeout, Counter bufferedMetrics, IngestionContext context) {
        super(threadPool);
        this.writer = writer;
        this.timeout = timeout;
        this.bufferedMetrics = bufferedMetrics;
        this.context = context;
    }
    
    @Override
    public ListenableFuture<List<Boolean>> apply(List<List<IMetric>> input) throws Exception {
        final long writeStartTime = System.currentTimeMillis();
        final Timer.Context actualWriteCtx = writeDurationTimer.time();
        
        final List<ListenableFuture<Boolean>> resultFutures = new ArrayList<ListenableFuture<Boolean>>();
        
        for (List<IMetric> metrics: input) {
            final int batchId = batchIdGenerator.next();
            final List<IMetric> batch = metrics;

            ListenableFuture<Boolean> futureBatchResult = getThreadPool().submit(new Callable<Boolean>() {
                public Boolean call() throws Exception {
                    final Timer.Context singleBatchWriteCtx = batchWriteDurationTimer.time();
                    try {
                        // break into Metric and PreaggregatedMetric, as the put paths are somewhat different.
                        // todo: AstyanaxWriter needs a refactored insertFull() method that takes a collection of metrics,
                        // susses out the string and boolean metrics for a different path, then segregates the Metric
                        // and Preaggregated metrics and writes them to the appropriate column families.
                        Collection<Metric> simpleMetrics = new ArrayList<Metric>();
                        Collection<IMetric> preagMetrics = new ArrayList<IMetric>();

                        for (IMetric m : batch) {
                            if (m instanceof Metric)
                                simpleMetrics.add((Metric) m);
                            else if (m instanceof PreaggregatedMetric)
                                preagMetrics.add(m);
                        }
                        if (simpleMetrics.size() > 0)
                            writer.insertFullMetrics(simpleMetrics);
                        if (preagMetrics.size() > 0)
                            writer.insertPreaggreatedMetrics(preagMetrics);

                        final Timer.Context dirtyTimerCtx = slotUpdateTimer.time();
                        try {
                            // marks this shard dirty, so rollup nodes know to pick up the work.
                            for (IMetric metric : batch) {
                                context.update(metric.getCollectionTime(), Util.getShard(metric.getLocator().toString()));
                            }
                        } finally {
                            dirtyTimerCtx.stop();
                        }

                        return true;
                    } catch (Exception ex) {
                        getLogger().error(ex.getMessage(), ex);
                        getLogger().warn("Did not persist all metrics successfully for batch " + batchId);
                        return false;
                    } finally {
                        singleBatchWriteCtx.stop();
                        bufferedMetrics.dec(batch.size());

                        if (System.currentTimeMillis() - writeStartTime > timeout.toMillis()) {
                            exceededScribeProcessingTime.mark();
                            getLogger().error("Exceeded timeout " + timeout.toString() + " before persisting " +
                                    "all metrics for batch " + batchId);
                        }
                    }
                }
            }); 
            
            resultFutures.add(futureBatchResult);
        }
        
        ListenableFuture<List<Boolean>> finalFuture = Futures.allAsList(resultFutures);
        finalFuture.addListener(new Runnable() {
            @Override
            public void run() {
                actualWriteCtx.stop();
            }
        }, MoreExecutors.sameThreadExecutor());
        return finalFuture;
    }
    
    private static class BatchIdGenerator {
        private int next = 0;
        
        public synchronized int next() {
            int id = next;
            
            if (next == Integer.MAX_VALUE) {
                next = 0;
                return next;
            } else {
                next += 1;
            }
            
            return id;
        }
    }
}
