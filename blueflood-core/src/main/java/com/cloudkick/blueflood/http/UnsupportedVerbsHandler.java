package com.cloudkick.blueflood.http;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

public class UnsupportedVerbsHandler implements HttpRequestHandler {

    @Override
    public void handle(ChannelHandlerContext ctx, HttpRequest request) {
        HttpResponder.respond(ctx, request, HttpResponseStatus.NOT_IMPLEMENTED);
    }
}