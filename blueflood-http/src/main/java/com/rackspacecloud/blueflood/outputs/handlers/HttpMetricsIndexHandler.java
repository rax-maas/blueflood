package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.http.HTTPRequestWithDecodedQueryParams;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.http.HttpResponder;
import com.rackspacecloud.blueflood.io.Constants;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.*;

import java.util.List;

public class HttpMetricsIndexHandler implements HttpRequestHandler{
    @Override
    public void handle(ChannelHandlerContext ctx, HttpRequest request) {
        final String tenantId = request.getHeader("tenantId");

        HTTPRequestWithDecodedQueryParams requestWithParams = (HTTPRequestWithDecodedQueryParams) request;
        List<String> query = requestWithParams.getQueryParams().get("query");

        if (query == null || query.size() != 1) {
            sendResponse(ctx, request, "Invalid Query String",
                    HttpResponseStatus.BAD_REQUEST);
            return;
        }

    }

    private void sendResponse(ChannelHandlerContext channel, HttpRequest request, String messageBody,
                              HttpResponseStatus status) {
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);

        if (messageBody != null && !messageBody.isEmpty()) {
            response.setContent(ChannelBuffers.copiedBuffer(messageBody, Constants.DEFAULT_CHARSET));
        }
        HttpResponder.respond(channel, request, response);
    }
}
