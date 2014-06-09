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
import com.rackspacecloud.blueflood.io.IMetricsWriter;
import com.rackspacecloud.blueflood.service.*;
import com.rackspacecloud.blueflood.types.MetricsCollection;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpContentDecompressor;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.jboss.netty.channel.Channels.pipeline;

public class HttpMetricsIngestionServer {
    private static final Logger log = LoggerFactory.getLogger(HttpMetricsIngestionServer.class);
    private static TimeValue DEFAULT_TIMEOUT = new TimeValue(5, TimeUnit.SECONDS);
    private static int WRITE_THREADS = Configuration.getInstance().getIntegerProperty(CoreConfig.METRICS_BATCH_WRITER_THREADS); // metrics will be batched into this many partitions.

    private int httpIngestPort;
    private String httpIngestHost;
    private TimeValue timeout;
    private IncomingMetricMetadataAnalyzer metricMetadataAnalyzer =
            new IncomingMetricMetadataAnalyzer(MetadataCache.getInstance());
    private ScheduleContext context;
    private final Counter bufferedMetrics = Metrics.counter(HttpMetricsIngestionServer.class, "Buffered Metrics");
    private static int MAX_CONTENT_LENGTH = 1048576; // 1 MB
    private static int BATCH_SIZE = Configuration.getInstance().getIntegerProperty(CoreConfig.METRIC_BATCH_SIZE);
    private int HTTP_MAX_TYPE_UNIT_PROCESSOR_THREADS = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_MAX_TYPE_UNIT_PROCESSOR_THREADS);
    
    private AsyncChain<MetricsCollection, List<Boolean>> defaultProcessorChain;
    private AsyncChain<String, List<Boolean>> statsdProcessorChain;
    private IMetricsWriter writer;

    public HttpMetricsIngestionServer(ScheduleContext context, IMetricsWriter writer) {
        this.httpIngestPort = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_INGESTION_PORT);
        this.httpIngestHost = Configuration.getInstance().getStringProperty(HttpConfig.HTTP_INGESTION_HOST);
        int acceptThreads = Configuration.getInstance().getIntegerProperty(HttpConfig.MAX_WRITE_ACCEPT_THREADS);
        int workerThreads = Configuration.getInstance().getIntegerProperty(HttpConfig.MAX_WRITE_WORKER_THREADS);
        this.timeout = DEFAULT_TIMEOUT; //TODO: make configurable
        this.context = context;
        this.writer = writer;
        
        buildProcessingChains();
        if (defaultProcessorChain == null || statsdProcessorChain == null) {
            log.error("Processor chains were not set up properly");
            return;
        }
        
        RouteMatcher router = new RouteMatcher();
        router.get("/v1.0", new DefaultHandler());

        router.post("/v1.0/multitenant/experimental/metrics", new HttpMultitenantMetricsIngestionHandler(defaultProcessorChain, timeout));
        router.post("/v1.0/:tenantId/experimental/metrics", new HttpMetricsIngestionHandler(defaultProcessorChain, timeout));

        router.post("/v1.0/:tenantId/experimental/metrics/statsd", new HttpStatsDIngestionHandler(statsdProcessorChain, timeout));


        log.info("Starting metrics listener HTTP server on port {}", httpIngestPort);
        ServerBootstrap server = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                        Executors.newFixedThreadPool(acceptThreads),
                        Executors.newFixedThreadPool(workerThreads)));

        server.setPipelineFactory(new MetricsHttpServerPipelineFactory(router));
        server.bind(new InetSocketAddress(httpIngestHost, httpIngestPort));
    }
    
    private void buildProcessingChains() {
        final AsyncFunction typeAndUnitProcessor; 
        final AsyncFunction batchSplitter;        
        final AsyncFunction batchWriter; 
        final AsyncFunction rollupTypeCacher;
        
        typeAndUnitProcessor = new TypeAndUnitProcessor(
                new ThreadPoolBuilder()
                        .withName("Metric type and unit processing")
                        .withCorePoolSize(HTTP_MAX_TYPE_UNIT_PROCESSOR_THREADS)
                        .withMaxPoolSize(HTTP_MAX_TYPE_UNIT_PROCESSOR_THREADS)
                        .build(),
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
                writer,
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

    private class MetricsHttpServerPipelineFactory implements ChannelPipelineFactory {
        private RouteMatcher router;

        public MetricsHttpServerPipelineFactory(RouteMatcher router) {
            this.router = router;
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            final ChannelPipeline pipeline = pipeline();

            pipeline.addLast("decoder", new HttpRequestDecoder() {
                
                // if something bad happens during the decode, assume the client send bad data. return a 400.
                @Override
                public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
                    ctx.getChannel().write(
                            new DefaultHttpResponse(HttpVersion.HTTP_1_1,HttpResponseStatus.BAD_REQUEST))
                            .addListener(ChannelFutureListener.CLOSE);
                }
            });
            pipeline.addLast("chunkaggregator", new HttpChunkAggregator(MAX_CONTENT_LENGTH));
            pipeline.addLast("inflater", new HttpContentDecompressor());
            pipeline.addLast("encoder", new HttpResponseEncoder());
            pipeline.addLast("encoder2", new HttpResponseDecoder());
            pipeline.addLast("handler", new QueryStringDecoderAndRouter(router));

            return pipeline;
        }
    }
}
