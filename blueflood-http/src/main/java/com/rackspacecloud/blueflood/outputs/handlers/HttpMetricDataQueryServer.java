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

import com.rackspacecloud.blueflood.http.DefaultHandler;
import com.rackspacecloud.blueflood.http.QueryStringDecoderAndRouter;
import com.rackspacecloud.blueflood.http.RouteMatcher;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.HttpConfig;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetSocketAddress;

public class HttpMetricDataQueryServer {
    private static final Logger log = LoggerFactory.getLogger(HttpMetricDataQueryServer.class);
    private final int httpQueryPort;
    private final String httpQueryHost;
    private static int MAX_CONTENT_LENGTH = 1048576; // 1 MB

    public HttpMetricDataQueryServer() {
        this.httpQueryPort = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_METRIC_DATA_QUERY_PORT);
        this.httpQueryHost = Configuration.getInstance().getStringProperty(HttpConfig.HTTP_QUERY_HOST);
        int acceptThreads = Configuration.getInstance().getIntegerProperty(HttpConfig.MAX_READ_ACCEPT_THREADS);
        int workerThreads = Configuration.getInstance().getIntegerProperty(HttpConfig.MAX_READ_WORKER_THREADS);

        final RouteMatcher router = new RouteMatcher();
        router.get("/v1.0", new DefaultHandler());
        router.get("/v1.0/:tenantId/experimental/views/metric_data/:metricName", new HttpRollupsQueryHandler());
        router.post("/v1.0/:tenantId/experimental/views/metric_data", new HttpMultiRollupsQueryHandler());
        router.get("/v1.0/:tenantId/experimental/views/histograms/:metricName", new HttpHistogramQueryHandler());

        log.info("Starting metric data query server (HTTP) on port {}", this.httpQueryPort);

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
                                    .addLast("encoder", new HttpResponseEncoder())
                                    .addLast("handler", new QueryStringDecoderAndRouter(router));
                        }
                    });
            b.bind(new InetSocketAddress(httpQueryHost, httpQueryPort)).sync();
        } catch (InterruptedException e) {
            log.error("Http Query Server interrupted while binding to {}", new InetSocketAddress(httpQueryHost, httpQueryPort));
            throw new RuntimeException(e);
        }
    }
}
