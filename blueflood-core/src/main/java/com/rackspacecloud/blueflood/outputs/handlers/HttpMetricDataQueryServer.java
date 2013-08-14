package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.http.DefaultHandler;
import com.rackspacecloud.blueflood.http.QueryStringDecoderAndRouter;
import com.rackspacecloud.blueflood.http.RouteMatcher;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import static org.jboss.netty.channel.Channels.pipeline;

public class HttpMetricDataQueryServer {
    private static final Logger log = LoggerFactory.getLogger(HttpMetricDataQueryServer.class);
    private final int httpQueryPort;
    private AstyanaxReader reader = AstyanaxReader.getInstance();

    public HttpMetricDataQueryServer(Integer portToListen) {
        this.httpQueryPort = portToListen;

        RouteMatcher router = new RouteMatcher();
        router.get("/v1.0", new DefaultHandler());
        router.get("/v1.0/:tenantId/experimental/views/metric_data/:metricName", new HttpRollupsQueryHandler());

        log.info("Starting metric data query server (HTTP) on port {}", this.httpQueryPort);
        ServerBootstrap server = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));
        server.setPipelineFactory(new MetricsHttpServerPipelineFactory(router));
        server.bind(new InetSocketAddress(httpQueryPort));
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
}
