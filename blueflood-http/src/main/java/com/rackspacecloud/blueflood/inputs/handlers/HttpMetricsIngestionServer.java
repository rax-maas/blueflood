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

import com.codahale.metrics.Counter;
import com.google.common.util.concurrent.AsyncFunction;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.concurrent.AsyncChain;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.http.DefaultHandler;
import com.rackspacecloud.blueflood.http.QueryStringDecoderAndRouter;
import com.rackspacecloud.blueflood.http.RouteMatcher;
import com.rackspacecloud.blueflood.inputs.processors.BatchSplitter;
import com.rackspacecloud.blueflood.inputs.processors.BatchWriter;
import com.rackspacecloud.blueflood.inputs.processors.RollupTypeCacher;
import com.rackspacecloud.blueflood.inputs.processors.TypeAndUnitProcessor;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.service.*;
import com.rackspacecloud.blueflood.types.MetricsCollection;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.TimeValue;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;

public class HttpMetricsIngestionServer {
    private static final Logger log = LoggerFactory.getLogger(HttpMetricsIngestionServer.class);
    private static TimeValue DEFAULT_TIMEOUT = new TimeValue(5, TimeUnit.SECONDS);
    private static int WRITE_THREADS = 50; // metrics will be batched into this many partitions.

    private int httpIngestPort;
    private String httpIngestHost;
    private TimeValue timeout;
    private IncomingMetricMetadataAnalyzer metricMetadataAnalyzer =
            new IncomingMetricMetadataAnalyzer(MetadataCache.getInstance());
    private ScheduleContext context;
    private final Counter bufferedMetrics = Metrics.counter(HttpMetricsIngestionServer.class, "Buffered Metrics");
    private static int MAX_CONTENT_LENGTH = 1048576; // 1 MB
    private static int BATCH_SIZE = Configuration.getInstance().getIntegerProperty(CoreConfig.METRIC_BATCH_SIZE);
    private static final int acceptThreads = Configuration.getInstance().getIntegerProperty(HttpConfig.MAX_WRITE_ACCEPT_THREADS);
    private static final int workerThreads = Configuration.getInstance().getIntegerProperty(HttpConfig.MAX_WRITE_WORKER_THREADS);
    
    private AsyncChain<MetricsCollection, List<Boolean>> defaultProcessorChain;
    private AsyncChain<String, List<Boolean>> statsdProcessorChain;
    private RouteMatcher router;

    public HttpMetricsIngestionServer(ScheduleContext context) {
        this.httpIngestPort = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_INGESTION_PORT);
        this.httpIngestHost = Configuration.getInstance().getStringProperty(HttpConfig.HTTP_INGESTION_HOST);
        this.timeout = DEFAULT_TIMEOUT; //TODO: make configurable
        this.context = context;
        
        buildProcessingChains();

        if (defaultProcessorChain == null || statsdProcessorChain == null) {
            log.error("Processor chains were not set up propertly");
            throw new RuntimeException("Ingestion Processor chains were not initialized properly");
        }

        initRouteMatcher();
    }

    public void start() {
        log.info("Starting metrics listener HTTP server on port {}", httpIngestPort);
        EventLoopGroup bossGroup = new NioEventLoopGroup(acceptThreads);
        EventLoopGroup workerGroup = new NioEventLoopGroup(workerThreads);
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel nioServerSocketChannel) throws Exception {
                            nioServerSocketChannel.pipeline()
                                    .addLast("decoder", new HttpRequestDecoder())
                                    .addLast("chunkaggregator", new HttpObjectAggregator(MAX_CONTENT_LENGTH))
                                    //.addLast("inflater", new HttpContentDecompressor())
                                    .addLast("encoder", new HttpResponseEncoder())
                                    .addLast("handler", new QueryStringDecoderAndRouter(router));
                        }
            });
            b.bind(new InetSocketAddress(httpIngestHost, httpIngestPort)).sync();
        } catch (InterruptedException e) {
            log.error("Http Ingest Server interrupted while binding to {}", new InetSocketAddress(httpIngestHost, httpIngestPort));
            throw new RuntimeException(e);
        }
    }
    
    private void buildProcessingChains() {
        final AsyncFunction typeAndUnitProcessor; 
        final AsyncFunction batchSplitter;        
        final AsyncFunction batchWriter; 
        final AsyncFunction rollupTypeCacher;
        
        typeAndUnitProcessor = new TypeAndUnitProcessor(
                new ThreadPoolBuilder().withName("Metric type and unit processing").build(),
                metricMetadataAnalyzer
        ).withLogger(log);
        
        batchSplitter = new BatchSplitter(
                new ThreadPoolBuilder().withName("Metric batching").build(),
                BATCH_SIZE
        ).withLogger(log);
        
        batchWriter = new BatchWriter(
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
            context
        ).withLogger(log);
        
        // RollupRunnable keeps a static one of these. It would be nice if we could register it and share.
        MetadataCache rollupTypeCache = MetadataCache.createLoadingCacheInstance(
                new TimeValue(48, TimeUnit.HOURS),
                Configuration.getInstance().getIntegerProperty(CoreConfig.MAX_ROLLUP_READ_THREADS));
        rollupTypeCacher = new RollupTypeCacher(
                new ThreadPoolBuilder().withName("Rollup type persistence").build(),
                rollupTypeCache,
                true
        ).withLogger(log);
        
        this.defaultProcessorChain = new AsyncChain<MetricsCollection, List<Boolean>>()
                .withFunction(typeAndUnitProcessor)
                .withFunction(rollupTypeCacher)
                .withFunction(batchSplitter)
                .withFunction(batchWriter);
        
        this.statsdProcessorChain = new AsyncChain<String, List<Boolean>>()
                .withFunction(new HttpStatsDIngestionHandler.MakeBundle())
                .withFunction(new HttpStatsDIngestionHandler.MakeCollection())
                .withFunction(typeAndUnitProcessor)
                .withFunction(rollupTypeCacher)
                .withFunction(batchSplitter)
                .withFunction(batchWriter);
    }

    private void initRouteMatcher() {
        this.router = new RouteMatcher();
        this.router.get("/v1.0", new DefaultHandler());
        this.router.post("/v1.0/:tenantId/experimental/metrics", new HttpMetricsIngestionHandler(defaultProcessorChain, timeout));
        this.router.post("/v1.0/:tenantId/experimental/metrics/statsd", new HttpStatsDIngestionHandler(statsdProcessorChain, timeout));
    }
}
