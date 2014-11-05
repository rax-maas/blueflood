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

import com.google.common.util.concurrent.AsyncFunction;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.concurrent.AsyncChain;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.http.DefaultHandler;
import com.rackspacecloud.blueflood.http.QueryStringDecoderAndRouter;
import com.rackspacecloud.blueflood.http.RouteMatcher;
import com.rackspacecloud.blueflood.io.IMetricsWriter;
import com.rackspacecloud.blueflood.service.*;
import com.rackspacecloud.blueflood.types.IMetric;
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
import java.util.concurrent.TimeUnit;

import static org.jboss.netty.channel.Channels.pipeline;

public class HttpMetricsIngestionServer {
    private static final Logger log = LoggerFactory.getLogger(HttpMetricsIngestionServer.class);
    private int httpIngestPort;
    private String httpIngestHost;

    private static int MAX_CONTENT_LENGTH = 1048576; // 1 MB
    
    private AsyncChain<MetricsCollection, List<Boolean>> defaultProcessorChain;
    private AsyncChain<String, List<Boolean>> statsdProcessorChain;

    public HttpMetricsIngestionServer(ScheduleContext context, IMetricsWriter writer) {
        this.httpIngestPort = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_INGESTION_PORT);
        this.httpIngestHost = Configuration.getInstance().getStringProperty(HttpConfig.HTTP_INGESTION_HOST);
        int acceptThreads = Configuration.getInstance().getIntegerProperty(HttpConfig.MAX_WRITE_ACCEPT_THREADS);
        int workerThreads = Configuration.getInstance().getIntegerProperty(HttpConfig.MAX_WRITE_WORKER_THREADS);
        
        RouteMatcher router = new RouteMatcher();
        router.get("/v1.0", new DefaultHandler());
        router.post("/v1.0/multitenant/experimental/metrics", new HttpMultitenantMetricsIngestionHandler(context, writer));
        router.post("/v1.0/:tenantId/experimental/metrics", new HttpMetricsIngestionHandler(context, writer));
        router.post("/v1.0/:tenantId/experimental/metrics/statsd", new HttpStatsDIngestionHandler(statsdProcessorChain));

        router.get("/v2.0", new DefaultHandler());
        router.post("/v2.0/:tenantId/ingest/multi", new HttpMultitenantMetricsIngestionHandler(context, writer));
        router.post("/v2.0/:tenantId/ingest", new HttpMetricsIngestionHandler(context, writer));
        router.post("/v2.0/:tenantId/ingest/aggregated", new HttpStatsDIngestionHandler(statsdProcessorChain));

        log.info("Starting metrics listener HTTP server on port {}", httpIngestPort);
        ServerBootstrap server = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                        Executors.newFixedThreadPool(acceptThreads),
                        Executors.newFixedThreadPool(workerThreads)));

        server.setPipelineFactory(new MetricsHttpServerPipelineFactory(router));
        server.bind(new InetSocketAddress(httpIngestHost, httpIngestPort));
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
