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
import com.rackspacecloud.blueflood.http.DefaultHandler;
import com.rackspacecloud.blueflood.http.QueryStringDecoderAndRouter;
import com.rackspacecloud.blueflood.http.RouteMatcher;
import com.rackspacecloud.blueflood.io.EventsIO;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.service.HttpConfig;
import com.rackspacecloud.blueflood.utils.ModuleLoader;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.channel.ServerChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.jboss.netty.channel.Channels.pipeline;

public class HttpMetricDataQueryServer {
    private static final Logger log = LoggerFactory.getLogger(HttpMetricDataQueryServer.class);
    private final int httpQueryPort;
    private final String httpQueryHost;
    private ServerChannel serverChannel;
    private EventsIO eventsIO;

    public HttpMetricDataQueryServer() {
        this.httpQueryPort = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_METRIC_DATA_QUERY_PORT);
        this.httpQueryHost = Configuration.getInstance().getStringProperty(HttpConfig.HTTP_QUERY_HOST);
    }

    public void startServer() {
        int acceptThreads = Configuration.getInstance().getIntegerProperty(HttpConfig.MAX_READ_ACCEPT_THREADS);
        int workerThreads = Configuration.getInstance().getIntegerProperty(HttpConfig.MAX_READ_WORKER_THREADS);

        RouteMatcher router = new RouteMatcher();
        router.get("/v1.0", new DefaultHandler());
        router.get("/v1.0/:tenantId/experimental/views/metric_data/:metricName", new HttpRollupsQueryHandler());
        router.post("/v1.0/:tenantId/experimental/views/metric_data", new HttpMultiRollupsQueryHandler());
        router.get("/v1.0/:tenantId/experimental/views/histograms/:metricName", new HttpHistogramQueryHandler());

        router.get("/v2.0", new DefaultHandler());
        router.get("/v2.0/:tenantId/views/:metricName", new HttpRollupsQueryHandler());
        router.post("/v2.0/:tenantId/views", new HttpMultiRollupsQueryHandler());
        router.get("/v2.0/:tenantId/views/histograms/:metricName", new HttpHistogramQueryHandler());
        router.get("/v2.0/:tenantId/metrics/search", new HttpMetricsIndexHandler());
        router.get("/v2.0/:tenantId/events/getEvents", new HttpEventsQueryHandler(getEventsIO()));
        router.options("/v2.0/:tenantId/events/getEvents", new HttpEventsQueryHandler(getEventsIO()));

        log.info("Starting metric data query server (HTTP) on port {}", this.httpQueryPort);
        ServerBootstrap server = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                        Executors.newFixedThreadPool(acceptThreads),
                        Executors.newFixedThreadPool(workerThreads)));
        server.setPipelineFactory(new MetricsHttpServerPipelineFactory(router));
        serverChannel =  (ServerChannel) server.bind(new InetSocketAddress(httpQueryHost, httpQueryPort));
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
            pipeline.addLast("handler", new QueryStringDecoderAndRouter(router));

            return pipeline;
        }
    }

    @VisibleForTesting
    public void stopServer() {
        try {
            serverChannel.close().await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // Pass
        }
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
