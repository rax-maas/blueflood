package com.rackspacecloud.blueflood.http;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

public class NoRouteHandler implements HttpRequestHandler {

    @Override
    public void handle(ChannelHandlerContext context, HttpRequest request) {
        HttpResponder.respond(context, request, HttpResponseStatus.NOT_FOUND);
    }
}