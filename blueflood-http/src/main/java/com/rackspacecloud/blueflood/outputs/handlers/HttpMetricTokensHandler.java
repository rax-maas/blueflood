package com.rackspacecloud.blueflood.outputs.handlers;

import com.codahale.metrics.Timer;
import com.rackspacecloud.blueflood.http.*;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.io.MetricToken;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.tracker.Tracker;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.ModuleLoader;
import io.netty.buffer.Unpooled;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HttpMetricTokensHandler implements HttpRequestHandler {
    private static final Logger log = LoggerFactory.getLogger(HttpMetricTokensHandler.class);
    private DiscoveryIO discoveryHandle;

    public HttpMetricTokensHandler() {
        discoveryHandle = (DiscoveryIO) ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.ENUMS_DISCOVERY_MODULES);
    }

    private final com.codahale.metrics.Timer HttpMetricNameTokensHandlerTimer = Metrics.timer(HttpMetricTokensHandler.class,
            "Handle HTTP request for getMetricTokens");

    public HttpMetricTokensHandler(DiscoveryIO discoveryHandle) {
        this.discoveryHandle = discoveryHandle;
    }

    @Override
    public void handle(ChannelHandlerContext ctx, FullHttpRequest request) {

        Tracker.getInstance().track(request);

        final Timer.Context httpMetricNameTokensHandlerTimerContext = HttpMetricNameTokensHandlerTimer.time();

        final String tenantId = request.headers().get("tenantId");

        HttpRequestWithDecodedQueryParams requestWithParams = (HttpRequestWithDecodedQueryParams) request;

        // get the query param
        List<String> query = requestWithParams.getQueryParams().get("query");
        if (query == null || query.size() != 1) {
            DefaultHandler.sendErrorResponse(ctx, request, "Invalid Query String",
                    HttpResponseStatus.BAD_REQUEST);
            return;
        }

        if (discoveryHandle == null) {
            sendResponse(ctx, request, null, HttpResponseStatus.NOT_FOUND);
            return;
        }

        try {
            List<MetricToken> metricTokens = discoveryHandle.getMetricTokens(tenantId, query.get(0));
            sendResponse(ctx, request, getSerializedJSON(metricTokens), HttpResponseStatus.OK);
        } catch (Exception e) {
            log.error(String.format("Exception occurred while trying to get metrics index for %s", tenantId), e);
            DefaultHandler.sendErrorResponse(ctx, request, "Error getting metrics index", HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } finally {
            httpMetricNameTokensHandlerTimerContext.stop();
        }
    }

    private void sendResponse(ChannelHandlerContext channel, FullHttpRequest request, String messageBody,
                              HttpResponseStatus status) {

        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status);

        if (messageBody != null && !messageBody.isEmpty()) {
            response.content().writeBytes(Unpooled.copiedBuffer(messageBody, Constants.DEFAULT_CHARSET));
        }

        HttpResponder.respond(channel, request, response);
        Tracker.getInstance().trackResponse(request, response);
    }

    public String getSerializedJSON(final List<MetricToken> metricTokens) {

        ArrayNode tokenInfoArrayNode = JsonNodeFactory.instance.arrayNode();
        for (MetricToken metricToken : metricTokens) {

            ObjectNode tokenInfoNode = JsonNodeFactory.instance.objectNode();

            tokenInfoNode.put(metricToken.getPath(), JsonNodeFactory.instance.booleanNode(metricToken.isLeaf()));

            tokenInfoArrayNode.add(tokenInfoNode);
        }

        return tokenInfoArrayNode.toString();
    }
}
