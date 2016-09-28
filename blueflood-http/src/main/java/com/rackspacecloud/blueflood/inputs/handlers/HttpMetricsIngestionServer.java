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

package com.rackspacecloud.blueflood.inputs.handlers;

import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.http.DefaultHandler;
import com.rackspacecloud.blueflood.http.QueryStringDecoderAndRouter;
import com.rackspacecloud.blueflood.http.RouteMatcher;
import com.rackspacecloud.blueflood.inputs.processors.DiscoveryWriter;
import com.rackspacecloud.blueflood.inputs.processors.BatchWriter;
import com.rackspacecloud.blueflood.inputs.processors.RollupTypeCacher;
import com.rackspacecloud.blueflood.inputs.processors.TypeAndUnitProcessor;
import com.rackspacecloud.blueflood.io.EventsIO;
import com.rackspacecloud.blueflood.service.*;
import com.rackspacecloud.blueflood.tracker.Tracker;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.MetricsCollection;
import com.rackspacecloud.blueflood.utils.ModuleLoader;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.TimeValue;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class HttpMetricsIngestionServer {
    private static final Logger log = LoggerFactory.getLogger(HttpMetricsIngestionServer.class);
    private static TimeValue DEFAULT_TIMEOUT = new TimeValue(5, TimeUnit.SECONDS);
    private int httpIngestPort;
    private String httpIngestHost;
    private Processor processor;
    private HttpEventsIngestionHandler httpEventsIngestionHandler;
    private final int httpMaxContentLength;

    private TimeValue timeout;

    private EventLoopGroup acceptorGroup;
    private EventLoopGroup workerGroup;
    private ChannelGroup allOpenChannels = new DefaultChannelGroup("allOpenChannels", GlobalEventExecutor.INSTANCE);

    /**
     * Constructor. Instantiate Metrics Ingest server
     * @param context
     */
    public HttpMetricsIngestionServer(ScheduleContext context) {
        this.httpIngestPort = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_INGESTION_PORT);
        this.httpIngestHost = Configuration.getInstance().getStringProperty(HttpConfig.HTTP_INGESTION_HOST);
        this.timeout = DEFAULT_TIMEOUT; //TODO: make configurable
        this.processor = new Processor(context, timeout);
        this.httpMaxContentLength = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_MAX_CONTENT_LENGTH);

        int acceptThreads = Configuration.getInstance().getIntegerProperty(HttpConfig.MAX_WRITE_ACCEPT_THREADS);
        int workerThreads = Configuration.getInstance().getIntegerProperty(HttpConfig.MAX_WRITE_WORKER_THREADS);
        acceptorGroup = new NioEventLoopGroup(acceptThreads); // acceptor threads
        workerGroup = new NioEventLoopGroup(workerThreads);   // client connections threads
    }

    /**
     * Starts the Ingest server
     *
     * @throws InterruptedException
     */
    public void startServer() throws InterruptedException {


        RouteMatcher router = new RouteMatcher();
        router.get("/v1.0", new DefaultHandler());
        router.post("/v1.0/multitenant/experimental/metrics", new HttpMultitenantMetricsIngestionHandler(processor, timeout));
        router.post("/v1.0/:tenantId/experimental/metrics", new HttpMetricsIngestionHandler(processor, timeout));
        router.post("/v1.0/:tenantId/experimental/metrics/statsd", new HttpAggregatedIngestionHandler(processor, timeout));

        router.get("/v2.0", new DefaultHandler());
        router.post("/v2.0/:tenantId/ingest/multi", new HttpMultitenantMetricsIngestionHandler(processor, timeout));
        router.post("/v2.0/:tenantId/ingest", new HttpMetricsIngestionHandler(processor, timeout));
        router.post("/v2.0/:tenantId/ingest/aggregated", new HttpAggregatedIngestionHandler(processor, timeout));
        router.post("/v2.0/:tenantId/events", getHttpEventsIngestionHandler());
        router.post("/v2.0/:tenantId/ingest/aggregated/multi", new HttpAggregatedMultiIngestionHandler(processor, timeout));
        final RouteMatcher finalRouter = router;

        log.info("Starting metrics listener HTTP server on port {}", httpIngestPort);
        ServerBootstrap server = new ServerBootstrap();
        server.group(acceptorGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel channel) throws Exception {
                        setupPipeline(channel, finalRouter);
                    }
                });

        Channel channel = server.bind(new InetSocketAddress(httpIngestHost, httpIngestPort)).sync().channel();
        allOpenChannels.add(channel);

        //register the tracker MBean for JMX/jolokia
        log.info("Registering tracker service");
        Tracker.getInstance().register();
    }

    private void setupPipeline(SocketChannel channel, RouteMatcher router) {
        final ChannelPipeline pipeline = channel.pipeline();

        pipeline.addLast("encoder", new HttpResponseEncoder());
        pipeline.addLast("decoder", new HttpRequestDecoder() {

            // if something bad happens during the decode, assume the client send bad data. return a 400.
            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable thr) throws Exception {
                try {
                    if (ctx.channel().isWritable()) {
                        log.debug("request decoder error " + thr.getCause().toString() + " on channel " + ctx.channel().toString());
                        ctx.channel().write(
                                new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST))
                                .addListener(ChannelFutureListener.CLOSE);
                    } else {
                        log.debug("channel " + ctx.channel().toString() + " is no longer writeable, not sending 400 response back to client");
                    }
                } catch (Exception ex) {
                    // If we are getting exception trying to write,
                    // don't propagate to caller. It may cause this
                    // method to be called again and will produce
                    // stack overflow. So just log it here.
                    log.debug("Can't write to channel " + ctx.channel().toString(), ex);
                }
            }
        });
        pipeline.addLast("inflater", new HttpContentDecompressor());
        pipeline.addLast("chunkaggregator", new HttpObjectAggregator(httpMaxContentLength));
        pipeline.addLast("respdecoder", new HttpResponseDecoder());
        pipeline.addLast("handler", new QueryStringDecoderAndRouter(router));
    }

    private HttpEventsIngestionHandler getHttpEventsIngestionHandler() {
        if (this.httpEventsIngestionHandler == null) {
            this.httpEventsIngestionHandler = new HttpEventsIngestionHandler((EventsIO) ModuleLoader.getInstance(EventsIO.class, CoreConfig.EVENTS_MODULES));
        }
        return this.httpEventsIngestionHandler;
    }

    @VisibleForTesting
    public void setHttpEventsIngestionHandler(HttpEventsIngestionHandler httpEventsIngestionHandler) {
        this.httpEventsIngestionHandler = httpEventsIngestionHandler;
    }

    static class Processor {
        private static int BATCH_SIZE = Configuration.getInstance().getIntegerProperty(CoreConfig.METRIC_BATCH_SIZE);
        private static int WRITE_THREADS = 
            Configuration.getInstance().getIntegerProperty(CoreConfig.METRICS_BATCH_WRITER_THREADS); // metrics will be batched into this many partitions.

        private final TypeAndUnitProcessor typeAndUnitProcessor;
        private final RollupTypeCacher rollupTypeCacher;
        private final DiscoveryWriter discoveryWriter;
        private final BatchWriter batchWriter;
        private IncomingMetricMetadataAnalyzer metricMetadataAnalyzer =
            new IncomingMetricMetadataAnalyzer(MetadataCache.getInstance());
        private int HTTP_MAX_TYPE_UNIT_PROCESSOR_THREADS = 
            Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_MAX_TYPE_UNIT_PROCESSOR_THREADS);
        private final Counter bufferedMetrics = Metrics.counter(HttpMetricsIngestionHandler.class, "Buffered Metrics");
        private final TimeValue timeout;

        Processor(ScheduleContext context, TimeValue timeout) {
            this.timeout = timeout;

            typeAndUnitProcessor = new TypeAndUnitProcessor(
                new ThreadPoolBuilder()
                    .withName("Metric type and unit processing")
                    .withCorePoolSize(HTTP_MAX_TYPE_UNIT_PROCESSOR_THREADS)
                    .withMaxPoolSize(HTTP_MAX_TYPE_UNIT_PROCESSOR_THREADS)
                    .build(),
                    metricMetadataAnalyzer);
            typeAndUnitProcessor.withLogger(log);

            batchWriter = new BatchWriter(
                    new ThreadPoolBuilder()
                            .withName("Metric Batch Writing")
                            .withCorePoolSize(WRITE_THREADS)
                            .withMaxPoolSize(WRITE_THREADS)
                            .withSynchronousQueue()
                            .build(),
                    timeout,
                    bufferedMetrics,
                    context
            );
            batchWriter.withLogger(log);

            discoveryWriter =
            new DiscoveryWriter(new ThreadPoolBuilder()
                .withName("Metric Discovery Writing")
                .withCorePoolSize(Configuration.getInstance().getIntegerProperty(CoreConfig.DISCOVERY_WRITER_MIN_THREADS))
                .withMaxPoolSize(Configuration.getInstance().getIntegerProperty(CoreConfig.DISCOVERY_WRITER_MAX_THREADS))
                .withUnboundedQueue()
                .build());
            discoveryWriter.withLogger(log);

            // RollupRunnable keeps a static one of these. It would be nice if we could register it and share.
            MetadataCache rollupTypeCache = MetadataCache.createLoadingCacheInstance(
                    new TimeValue(48, TimeUnit.HOURS),
                    Configuration.getInstance().getIntegerProperty(CoreConfig.MAX_ROLLUP_READ_THREADS));
            rollupTypeCacher = new RollupTypeCacher(
                    new ThreadPoolBuilder().withName("Rollup type persistence").build(),
                    rollupTypeCache);
            rollupTypeCacher.withLogger(log);
    
        }

        ListenableFuture<List<Boolean>> apply(MetricsCollection collection) throws Exception {
            typeAndUnitProcessor.apply(collection);
            rollupTypeCacher.apply(collection);
            List<List<IMetric>> batches = collection.splitMetricsIntoBatches(BATCH_SIZE);
            discoveryWriter.apply(batches);
            return batchWriter.apply(batches);
        }
    }

    @VisibleForTesting
    public void shutdownServer() {
        try {
            allOpenChannels.close().await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // Pass
        }
        acceptorGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }
}
