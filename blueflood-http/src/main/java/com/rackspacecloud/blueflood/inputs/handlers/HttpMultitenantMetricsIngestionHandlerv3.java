package com.rackspacecloud.blueflood.inputs.handlers;

import com.rackspacecloud.blueflood.concurrent.AsyncChain;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpRequest;

import java.util.List;

public class HttpMultitenantMetricsIngestionHandlerv3 implements HttpRequestHandler {
    public HttpMultitenantMetricsIngestionHandlerv3(AsyncChain<String, List<Boolean>> v3ProcessorChain, TimeValue timeout) {
    }

    @Override
    public void handle(ChannelHandlerContext ctx, HttpRequest request) {
       // TODO: Add tenantId to the scope of the ingestion payload
    }
}
