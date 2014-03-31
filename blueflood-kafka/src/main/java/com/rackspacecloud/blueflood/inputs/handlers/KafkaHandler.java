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

package com.rackspacecloud.blueflood.inputs.handlers;

import com.codahale.metrics.Counter;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.concurrent.AsyncChain;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.inputs.processors.*;
import com.rackspacecloud.blueflood.io.IMetricsWriter;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.service.IncomingMetricMetadataAnalyzer;
import com.rackspacecloud.blueflood.service.ScheduleContext;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.MetricsCollection;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class KafkaHandler {
    private static final Logger log = LoggerFactory.getLogger(KafkaHandler.class);
    private static final int BATCH_SIZE = Configuration.getInstance().getIntegerProperty(CoreConfig.METRIC_BATCH_SIZE);
    private static int WRITE_THREADS = 10; // metrics will be batched into this many partitions.
    private static final TimeValue timeout = new TimeValue(30l, TimeUnit.SECONDS);

    // NOTE: this is shared between a LogEntryConverter and a BatchWriter (former increments, latter decrements).
    private final Counter bufferedMetrics = Metrics.counter(KafkaHandler.class, "Buffered Metrics");
    private final AsyncFunctionWithThreadPool<MetricsCollection, MetricsCollection> rollupTypeCacher;
    private final AsyncFunctionWithThreadPool<MetricsCollection, MetricsCollection> typeAndUnitProcessor;
    private final AsyncFunctionWithThreadPool<MetricsCollection, List<List<IMetric>>> batchSplitter;
//    private final AsyncChain<List<LogEntry>, List<Boolean>> processorChain;

    private IncomingMetricMetadataAnalyzer metricMetadataAnalyzer = new IncomingMetricMetadataAnalyzer(MetadataCache.getInstance());
    private ScheduleContext context;

    private final BatchWriter batchWriter;

    public KafkaHandler(ScheduleContext context, IMetricsWriter writer) {
        this.context = context;

        batchWriter = new BatchWriter(
                new ThreadPoolBuilder()
                        .withName("Metric Batch Writing")
                        .withCorePoolSize(WRITE_THREADS)
                        .withMaxPoolSize(WRITE_THREADS)
                        .withUnboundedQueue()
                        .withRejectedHandler(new ThreadPoolExecutor.AbortPolicy())
                        .build(),
                writer, // todo: figure out how to have separate writers for separate ingestors
                timeout,
                bufferedMetrics,
                context);

        MetadataCache rollupTypeCache = MetadataCache.createLoadingCacheInstance(
                new TimeValue(48, TimeUnit.HOURS),
                Configuration.getInstance().getIntegerProperty(CoreConfig.MAX_ROLLUP_READ_THREADS));
        rollupTypeCacher = new RollupTypeCacher(
                new ThreadPoolBuilder().withName("Rollup type persistence").build(),
                rollupTypeCache,
                true
        ).withLogger(log);

        typeAndUnitProcessor = new TypeAndUnitProcessor(
                new ThreadPoolBuilder().withName("Metric type and unit processing")
                        .withCorePoolSize(WRITE_THREADS)
                        .withMaxPoolSize(WRITE_THREADS).build(),
                metricMetadataAnalyzer)
                .withLogger(log);

        batchSplitter = new BatchSplitter(
                new ThreadPoolBuilder().withName("Metric batching").build(),
                BATCH_SIZE
        ).withLogger(log);

        AsyncChain<MetricsCollection, List<Boolean>> processorChain = new AsyncChain<MetricsCollection, List<Boolean>>()
                .withFunction(typeAndUnitProcessor)
                .withFunction(rollupTypeCacher)
                .withFunction(batchSplitter)
                .withFunction(batchWriter);

        new KafkaConsumerWorker(bufferedMetrics, processorChain).run();
    }
}