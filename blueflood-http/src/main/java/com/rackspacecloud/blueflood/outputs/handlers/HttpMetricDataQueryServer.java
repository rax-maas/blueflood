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

package com.rackspacecloud.blueflood.outputs.handlers;

import com.google.common.annotations.VisibleForTesting;
import com.rackspacecloud.blueflood.http.*;
import com.rackspacecloud.blueflood.io.EventsIO;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.service.HttpConfig;
import com.rackspacecloud.blueflood.tracker.Tracker;
import com.rackspacecloud.blueflood.utils.ModuleLoader;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class HttpMetricDataQueryServer {
    private static final Logger log = LoggerFactory.getLogger(HttpMetricDataQueryServer.class);
    private final int httpQueryPort;
    private final String httpQueryHost;
    private final int httpMaxContentLength;
    private Channel serverChannel;
    private EventsIO eventsIO;
    private EventLoopGroup acceptorGroup;
    private EventLoopGroup workerGroup;

    public HttpMetricDataQueryServer() {
        this.httpQueryPort = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_METRIC_DATA_QUERY_PORT);
        this.httpQueryHost = Configuration.getInstance().getStringProperty(HttpConfig.HTTP_QUERY_HOST);
        this.httpMaxContentLength = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_MAX_CONTENT_LENGTH);

        int acceptThreads = Configuration.getInstance().getIntegerProperty(HttpConfig.MAX_READ_ACCEPT_THREADS);
        int workerThreads = Configuration.getInstance().getIntegerProperty(HttpConfig.MAX_READ_WORKER_THREADS);
        acceptorGroup = new NioEventLoopGroup(acceptThreads); // acceptor threads
        workerGroup = new NioEventLoopGroup(workerThreads);   // client connections threads
    }

    public void startServer() throws InterruptedException {

        RouteMatcher router = new RouteMatcher();
        router.get("/v1.0", new DefaultHandler());
        router.get("/v1.0/:tenantId/experimental/views/metric_data/:metricName", new HttpRollupsQueryHandler());

        router.post("/v1.0/:tenantId/experimental/views/metric_data", new HttpMultiRollupsQueryHandler());
        router.post("/v2.0/:tenantId/views", new HttpMultiRollupsQueryHandler());

        router.get("/v2.0", new DefaultHandler());
        router.get("/v2.0/:tenantId/views/:metricName", new HttpRollupsQueryHandler());
        router.get("/v2.0/:tenantId/metrics/search", new HttpMetricsIndexHandler());
        router.get("/v2.0/:tenantId/metric_name/search", new HttpMetricTokensHandler());
        router.get("/v2.0/:tenantId/events/getEvents", new HttpEventsQueryHandler(getEventsIO()));

        router.options("/v2.0/:tenantId/views/:metricName", new HttpOptionsHandler());
        router.options("/v2.0/:tenantId/views", new HttpOptionsHandler());
        router.options("/v2.0/:tenantId/metrics/search", new HttpOptionsHandler());
        router.options("/v2.0/:tenantId/metric_name/search", new HttpOptionsHandler());
        router.options("/v2.0/:tenantId/events/getEvents", new HttpOptionsHandler());

        final RouteMatcher finalRouter = router;

        log.info("Starting metric data query server (HTTP) on port {}", this.httpQueryPort);
        ServerBootstrap server = new ServerBootstrap();
        server.group(acceptorGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel channel) throws Exception {
                        setupPipeline(channel, finalRouter);
                    }
                });
        serverChannel = server.bind(new InetSocketAddress(httpQueryHost, httpQueryPort)).sync().channel();

        //register the tracker MBean for JMX/jolokia
        log.info("Registering tracker service");
        Tracker.getInstance().register();
    }

    private void setupPipeline(SocketChannel channel, RouteMatcher router) {
        final ChannelPipeline pipeline = channel.pipeline();

        pipeline.addLast("encoder", new HttpResponseEncoder());
        pipeline.addLast("decoder", new HttpRequestDecoder() {
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
        pipeline.addLast("chunkaggregator", new HttpObjectAggregator(httpMaxContentLength));
        pipeline.addLast("handler", new QueryStringDecoderAndRouter(router));
    }

    @VisibleForTesting
    public void stopServer() {
        try {
            serverChannel.close().await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // Pass
        }
        acceptorGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    private EventsIO getEventsIO() {
        if (this.eventsIO == null) {
            this.eventsIO = (EventsIO) ModuleLoader.getInstance(EventsIO.class, CoreConfig.EVENTS_MODULES);
        }

        return this.eventsIO;
    }

    @VisibleForTesting
    public void setEventsIO(EventsIO eventsIO) {
        this.eventsIO = eventsIO;
    }
}
