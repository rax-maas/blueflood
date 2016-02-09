package com.rackspacecloud.blueflood.outputs.handlers;

import com.codahale.metrics.Timer;
import com.rackspacecloud.blueflood.http.HTTPRequestWithDecodedQueryParams;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.http.HttpResponder;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.io.TokenInfo;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.tracker.Tracker;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.ModuleLoader;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HttpMetricNameTokensHandler implements HttpRequestHandler {
    private static final Logger log = LoggerFactory.getLogger(HttpMetricNameTokensHandler.class);
    private DiscoveryIO discoveryHandle;

    public HttpMetricNameTokensHandler() {
        discoveryHandle = (DiscoveryIO) ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.ENUMS_DISCOVERY_MODULES);
    }

    private final com.codahale.metrics.Timer HttpMetricNameTokensHandlerTimer = Metrics.timer(HttpMetricNameTokensHandler.class,
            "Handle HTTP request for getNextTokens");

    public HttpMetricNameTokensHandler(DiscoveryIO discoveryHandle) {
        this.discoveryHandle = discoveryHandle;
    }

    @Override
    public void handle(ChannelHandlerContext ctx, HttpRequest request) {

        final Timer.Context httpMetricNameTokensHandlerTimerContext = HttpMetricNameTokensHandlerTimer.time();

        Tracker.track(request);

        final String tenantId = request.getHeader("tenantId");

        HTTPRequestWithDecodedQueryParams requestWithParams = (HTTPRequestWithDecodedQueryParams) request;

        // get the prefix param
        List<String> prefixRequestParam = requestWithParams.getQueryParams().get("prefix");

        String prefix = "";
        if (prefixRequestParam != null) {

            if (prefixRequestParam.size() != 1) {
                sendResponse(ctx, request, "Invalid request parameter: prefix",
                        HttpResponseStatus.BAD_REQUEST);
                return;
            } else {
                prefix = prefixRequestParam.get(0);
            }
        }

        if (discoveryHandle == null) {
            sendResponse(ctx, request, null, HttpResponseStatus.NOT_FOUND);
            return;
        }

        try {
            List<TokenInfo> tokenInfos = discoveryHandle.getNextTokens(tenantId, prefix);
            sendResponse(ctx, request, getSerializedJSON(tokenInfos), HttpResponseStatus.OK);
        } catch (Exception e) {
            log.error(String.format("Exception occurred while trying to get metrics index for %s", tenantId), e);
            sendResponse(ctx, request, "Error getting metrics index", HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } finally {
            httpMetricNameTokensHandlerTimerContext.stop();
        }
    }

    private void sendResponse(ChannelHandlerContext channel, HttpRequest request, String messageBody,
                              HttpResponseStatus status) {

        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);

        if (messageBody != null && !messageBody.isEmpty()) {
            response.setContent(ChannelBuffers.copiedBuffer(messageBody, Constants.DEFAULT_CHARSET));
        }

        Tracker.trackResponse(request, response);
        HttpResponder.respond(channel, request, response);
    }

    public String getSerializedJSON(final List<TokenInfo> tokenInfos) {

        ArrayNode tokenInfoArrayNode = JsonNodeFactory.instance.arrayNode();
        for (TokenInfo tokenInfo: tokenInfos) {

            ObjectNode tokenInfoNode = JsonNodeFactory.instance.objectNode();

            tokenInfoNode.put(tokenInfo.getToken(), JsonNodeFactory.instance.booleanNode(tokenInfo.isNextLevel()));

            tokenInfoArrayNode.add(tokenInfoNode);
        }

        return tokenInfoArrayNode.toString();
    }
}
