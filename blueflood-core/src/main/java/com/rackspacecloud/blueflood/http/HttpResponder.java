package com.rackspacecloud.blueflood.http;

import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import static org.jboss.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static org.jboss.netty.handler.codec.http.HttpHeaders.setContentLength;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class HttpResponder {
    private static final DefaultHttpResponse defaultResponse = new DefaultHttpResponse(HTTP_1_1,
            HttpResponseStatus.OK);


    public static void respond(ChannelHandlerContext ctx, HttpRequest req, HttpResponseStatus status) {
        defaultResponse.setStatus(status);
        respond(ctx, req, defaultResponse);
    }

    public static void respond(ChannelHandlerContext ctx, HttpRequest req, HttpResponse res) {
        if (res.getContent() != null) {
            setContentLength(res, res.getContent().readableBytes());
        }

        // Send the response and close the connection if necessary.
        ChannelFuture f = ctx.getChannel().write(res);
        if (!isKeepAlive(req)) {
            f.addListener(ChannelFutureListener.CLOSE);
        }
    }
}