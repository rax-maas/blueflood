package com.rackspacecloud.blueflood.http;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpRequest;

public interface HttpRequestHandler {
    public void handle(ChannelHandlerContext ctx, HttpRequest request);
}