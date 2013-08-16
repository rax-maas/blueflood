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
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.service.IngestionContext;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.Util;
import com.rackspacecloud.blueflood.utils.TimeValue;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class BatchWriter extends AsyncFunctionWithThreadPool<List<List<Metric>>, List<Boolean>> {
        
    private final BatchIdGenerator batchIdGenerator = new BatchIdGenerator();
    // todo: CM_SPECIFIC verify changing metric class name doesn't break things.
    private final Timer writeDurationTimer = Metrics.newTimer(BatchWriter.class, "Write Duration", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private final Meter exceededScribeProcessingTime = Metrics.newMeter(BatchWriter.class, "Write Duration Exceeded Timeout", "Rollups", TimeUnit.SECONDS);
    private final TimeValue scribeTimeout;
    private final Counter bufferedMetrics;
    private final IngestionContext context;
    
    private final AstyanaxWriter writer;
    
    public BatchWriter(ThreadPoolExecutor threadPool, AstyanaxWriter writer, TimeValue scribeTimeout, Counter bufferedMetrics, IngestionContext context) {
        super(threadPool);
        this.writer = writer;
        this.scribeTimeout = scribeTimeout;
        this.bufferedMetrics = bufferedMetrics;
        this.context = context;
    }
    
    public ListenableFuture<List<Boolean>> apply(List<List<Metric>> input) throws Exception {
        
        final CountDownLatch shortLatch = new CountDownLatch(input.size());
        final AtomicBoolean successfullyPersisted = new AtomicBoolean(true);
        
        
        final AtomicBoolean writeTimedOut = new AtomicBoolean(false);
        final long writeStartTime = System.currentTimeMillis();
        final TimerContext actualWriteCtx = writeDurationTimer.time();
        
        final List<ListenableFuture<Boolean>> resultFutures = new ArrayList<ListenableFuture<Boolean>>();
        
        for (List<Metric> metrics: input) {
            final int batchId = batchIdGenerator.next();
            final List<Metric> batch = metrics;
            
            
            ListenableFuture<Boolean> futureBatchResult = getThreadPool().submit(new Callable<Boolean>() {
                public Boolean call() throws Exception {
                    try {
                        writer.insertFull(batch);
                        
                        // marks this shard dirty, so rollup nodes know to pick up the work.
                        for (Metric metric : batch) {
                            context.update(metric.getCollectionTime(), Util.computeShard(metric.getLocator().toString()));
                        }
                        
                        return true;
                    } catch (Exception ex) {
                        getLogger().error(ex.getMessage(), ex);
                        successfullyPersisted.set(false);
                        return false;
                    } finally {
                        shortLatch.countDown();
                        bufferedMetrics.dec(batch.size());
                        
                        if (System.currentTimeMillis() - writeStartTime > scribeTimeout.toMillis()) {
                            writeTimedOut.set(true);
                        }
                        done();
                    }
                    
                    
                }
                
                private void done() {
                    if (shortLatch.getCount() == 0) {
                        actualWriteCtx.stop();
    
                        if (writeTimedOut.get()) {
                            exceededScribeProcessingTime.mark();
                            getLogger().error("Exceeded scribe timeout " + scribeTimeout.toString() + " before persisting " +
                                    "all metrics for scribe batch " + batchId);
                        }
    
                        if (!successfullyPersisted.get()) {
                            getLogger().warn("Did not persist all metrics successfully for scribe batch " + batchId);
                        }
                    }
                }
                
            }); 
            
            resultFutures.add(futureBatchResult);
        }
        
        return Futures.allAsList(resultFutures);
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