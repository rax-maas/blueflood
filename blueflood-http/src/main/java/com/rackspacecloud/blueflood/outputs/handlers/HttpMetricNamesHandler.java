package com.rackspacecloud.blueflood.outputs.handlers;

import com.codahale.metrics.Timer;
import com.rackspacecloud.blueflood.http.*;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.tracker.Tracker;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.ModuleLoader;
import io.netty.buffer.Unpooled;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HttpMetricNamesHandler implements HttpRequestHandler {
    private static final Logger log = LoggerFactory.getLogger(HttpMetricNamesHandler.class);
    private MetricNameSearchIO discoveryHandle;

    public static boolean EXP_TOKEN_SEARCH_IMPROVEMENTS =
            Configuration.getInstance().getBooleanProperty(CoreConfig.ENABLE_TOKEN_SEARCH_IMPROVEMENTS);

    public HttpMetricNamesHandler() {
        log.info("Token search improvements enabled: " + EXP_TOKEN_SEARCH_IMPROVEMENTS);
        if (EXP_TOKEN_SEARCH_IMPROVEMENTS) {
            discoveryHandle = (TokenDiscoveryIO) ModuleLoader.getInstance(TokenDiscoveryIO.class, CoreConfig.TOKEN_DISCOVERY_MODULES);
        } else {
            discoveryHandle = (DiscoveryIO) ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.DISCOVERY_MODULES);
        }
    }

    private final com.codahale.metrics.Timer HttpMetricNamesHandlerTimer = Metrics.timer(HttpMetricNamesHandler.class,
                                                                                         "Handle HTTP request for getMetricNames");

    public HttpMetricNamesHandler(DiscoveryIO discoveryHandle) {
        this.discoveryHandle = discoveryHandle;
    }

    @Override
    public void handle(ChannelHandlerContext ctx, FullHttpRequest request) {

        Tracker.getInstance().track(request);

        final Timer.Context httpMetricNamesHandlerTimerContext = HttpMetricNamesHandlerTimer.time();

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
            List<MetricName> metricNames = discoveryHandle.getMetricNames(tenantId, query.get(0));
            sendResponse(ctx, request, getSerializedJSON(metricNames), HttpResponseStatus.OK);
        } catch (Exception e) {
            log.error(String.format("Exception occurred while trying to get metrics index for %s", tenantId), e);
            DefaultHandler.sendErrorResponse(ctx, request, "Error getting metrics index", HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } finally {
            httpMetricNamesHandlerTimerContext.stop();
        }
    }

    private void sendResponse(ChannelHandlerContext channel, FullHttpRequest request, String messageBody,
                              HttpResponseStatus status) {

        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status);

        if (messageBody != null && !messageBody.isEmpty()) {
            response.content().writeBytes(Unpooled.copiedBuffer(messageBody, Constants.DEFAULT_CHARSET));
        }

        HttpResponder.getInstance().respond(channel, request, response);
        Tracker.getInstance().trackResponse(request, response);
    }

    public String getSerializedJSON(final List<MetricName> metricNames) {

        ArrayNode tokenInfoArrayNode = JsonNodeFactory.instance.arrayNode();
        for (MetricName metricName : metricNames) {

            ObjectNode metricNameInfoNode = JsonNodeFactory.instance.objectNode();

            metricNameInfoNode.put(metricName.getName(), JsonNodeFactory.instance.booleanNode(metricName.isCompleteName()));

            tokenInfoArrayNode.add(metricNameInfoNode);
        }

        return tokenInfoArrayNode.toString();
    }
}
