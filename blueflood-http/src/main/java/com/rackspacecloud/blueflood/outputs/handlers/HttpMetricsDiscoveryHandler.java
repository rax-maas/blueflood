/*
 * Copyright 2014 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.outputs.handlers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.rackspacecloud.blueflood.http.HTTPRequestWithDecodedQueryParams;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.http.HttpResponder;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.io.ElasticIO;
import com.rackspacecloud.blueflood.outputs.serializers.JSONMetricsListOutputSerializer;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.apache.oro.text.GlobCompiler;
import org.apache.oro.text.regex.MalformedPatternException;
import org.elasticsearch.ElasticSearchException;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.*;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class HttpMetricsDiscoveryHandler implements HttpRequestHandler {
    private static final Logger log = LoggerFactory.getLogger(HttpMetricsDiscoveryHandler.class);
    private final ElasticIO elastiCIO;
    private final Gson gson;           // thread-safe
    private final JsonParser parser;   // thread-safe
    private final GlobCompiler globCompiler;

    private static final Timer httpMetricsListTimer = Metrics.newTimer(HttpMetricsDiscoveryHandler.class,
            "Handle HTTP request for metrics", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

    public HttpMetricsDiscoveryHandler() {
        this.gson = new GsonBuilder().setPrettyPrinting().serializeNulls().create();
        this.parser = new JsonParser();
        this.elastiCIO = new ElasticIO();
        this.globCompiler = new GlobCompiler();
    }

    @Override
    public void handle(ChannelHandlerContext ctx, HttpRequest request) {
        final String tenantId = request.getHeader("tenantId");

        HTTPRequestWithDecodedQueryParams requestWithParams = (HTTPRequestWithDecodedQueryParams) request;
        String filter = requestWithParams.getHeader("filter");

        if (filter == null || filter.isEmpty()) {
            filter = "*";
        }

        try {
            globCompiler.compile(filter.toCharArray());
        } catch (MalformedPatternException ex) {
            sendResponse(ctx, request, "Invalid glob pattern " + filter, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        final TimerContext httpMetricsListTimerContext = httpMetricsListTimer.time();
        try {
            List<ElasticIO.Result> results = elastiCIO.search(new ElasticIO.Discovery(tenantId, filter));
            final JSONObject payload = JSONMetricsListOutputSerializer.transform(results);
            final JsonElement element = parser.parse(payload.toString());
            final String jsonStringRep = gson.toJson(element);
            sendResponse(ctx, request, jsonStringRep, HttpResponseStatus.OK);
        } catch (ElasticSearchException ex) {
            log.error("Elastic search exception searching for metrics for tenant: " + tenantId, ex);
            sendResponse(ctx, request, ex.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception ex) {
            log.error("Exception searching for metrics for tenant: " + tenantId, ex);
            sendResponse(ctx, request, ex.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } finally {
            httpMetricsListTimerContext.stop();
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
