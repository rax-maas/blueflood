package com.rackspacecloud.blueflood.http;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryStringDecoderAndRouter extends SimpleChannelUpstreamHandler {
    private static final Logger log = LoggerFactory.getLogger(QueryStringDecoderAndRouter.class);
    private final RouteMatcher router;

    public QueryStringDecoderAndRouter(RouteMatcher router) {
        this.router = router;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        Object msg = e.getMessage();
        if (msg instanceof DefaultHttpRequest) {
            final DefaultHttpRequest request = (DefaultHttpRequest) msg;
            final QueryStringDecoder decoder = new QueryStringDecoder(((HttpRequest) msg).getUri());

            // Modify the original request headers with query parameters
            if (decoder != null && !decoder.getParameters().isEmpty()) {
                final HttpRequest requestWithParams =
                        HTTPRequestWithDecodedQueryParams.createHttpRequestWithDecodedQueryParams(request);
                router.route(ctx, requestWithParams);
            } else {
                router.route(ctx, request);
            }
        } else {
            log.error("Ignoring non HTTP message {}, from {}", e.getMessage(), e.getRemoteAddress());
            throw new Exception("Non-HTTP message from " + e.getRemoteAddress());
        }
    }
}
