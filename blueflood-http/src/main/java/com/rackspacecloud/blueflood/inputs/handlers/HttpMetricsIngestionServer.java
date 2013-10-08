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

package com.rackspacecloud.blueflood.inputs.handlers;

import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.concurrent.AsyncChain;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.http.DefaultHandler;
import com.rackspacecloud.blueflood.http.QueryStringDecoderAndRouter;
import com.rackspacecloud.blueflood.http.RouteMatcher;
import com.rackspacecloud.blueflood.inputs.processors.BatchSplitter;
import com.rackspacecloud.blueflood.inputs.processors.BatchWriter;
import com.rackspacecloud.blueflood.inputs.processors.TypeAndUnitProcessor;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.service.IncomingMetricMetadataAnalyzer;
import com.rackspacecloud.blueflood.service.ScheduleContext;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.IngestionService;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.MetricsCollection;
import com.rackspacecloud.blueflood.utils.TimeValue;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.jboss.netty.channel.Channels.pipeline;

public class HttpMetricsIngestionServer implements IngestionService {
    private static final Logger log = LoggerFactory.getLogger(HttpMetricsIngestionServer.class);
    private static TimeValue DEFAULT_TIMEOUT = new TimeValue(5, TimeUnit.SECONDS);
    private static int WRITE_THREADS = 50; // metrics will be batched into this many partitions.

    private int port;
    private AsyncChain<MetricsCollection, Boolean> processorChain;
    private TimeValue timeout;
    private IncomingMetricMetadataAnalyzer metricMetadataAnalyzer =
            new IncomingMetricMetadataAnalyzer(MetadataCache.getInstance());
    private ScheduleContext context;
    private final Counter bufferedMetrics = Metrics.newCounter(HttpMetricsIngestionServer.class, "Buffered Metrics");
    private static int MAX_CONTENT_LENGTH = 1048576; // 1 MB


    public HttpMetricsIngestionServer() {
        this.port = Configuration.getIntegerProperty("HTTP_INGESTION_PORT");
        this.timeout = DEFAULT_TIMEOUT; //TODO: make configurable

        this.processorChain = createDefaultProcessorChain();
    }

    public void startService(ScheduleContext context) {
        this.context = context;

        RouteMatcher router = new RouteMatcher();
        router.get("/v1.0", new DefaultHandler());
        router.post("/v1.0/:tenantId/experimental/metrics", new HttpMetricsIngestionHandler(this.processorChain, timeout));

        log.info("Starting metrics listener HTTP server on port {}", port);
        ServerBootstrap server = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));

        server.setPipelineFactory(new MetricsHttpServerPipelineFactory(router));
        server.bind(new InetSocketAddress(port));
    }

    private AsyncChain<MetricsCollection, Boolean> createDefaultProcessorChain() {
        return new AsyncChain<MetricsCollection, Boolean>()
                .withFunction(new TypeAndUnitProcessor(
                        new ThreadPoolBuilder().withName("Metric type and unit processing").build(),
                        metricMetadataAnalyzer)
                        .withLogger(log))
                .withFunction(new BatchSplitter(
                        new ThreadPoolBuilder().withName("Metric batching").build(),
                        WRITE_THREADS)
                        .withLogger(log))
                .withFunction(new BatchWriter(
                        new ThreadPoolBuilder()
                                .withName("Metric Batch Writing")
                                .withCorePoolSize(WRITE_THREADS)
                                .withMaxPoolSize(WRITE_THREADS)
                                .withUnboundedQueue()
                                .withRejectedHandler(new ThreadPoolExecutor.AbortPolicy())
                                .build(),
                        AstyanaxWriter.getInstance(),
                        timeout,
                        bufferedMetrics,
                        context)
                        .withLogger(log));
    }

    private class MetricsHttpServerPipelineFactory implements ChannelPipelineFactory {
        private RouteMatcher router;

        public MetricsHttpServerPipelineFactory(RouteMatcher router) {
            this.router = router;
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            final ChannelPipeline pipeline = pipeline();

            pipeline.addLast("decoder", new HttpRequestDecoder());
            pipeline.addLast("encoder", new HttpResponseEncoder());
            pipeline.addLast("chunkaggregator", new HttpChunkAggregator(MAX_CONTENT_LENGTH));
            pipeline.addLast("handler", new QueryStringDecoderAndRouter(router));

            return pipeline;
        }
    }
}
