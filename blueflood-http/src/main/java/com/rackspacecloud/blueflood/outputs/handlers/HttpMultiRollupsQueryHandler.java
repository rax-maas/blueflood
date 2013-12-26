/*
 * Copyright 2013 Rackspace
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

import com.google.gson.*;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.exceptions.InvalidRequestException;
import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.http.HTTPRequestWithDecodedQueryParams;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.http.HttpResponder;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.outputs.serializers.BatchedMetricsJSONOutputSerializer;
import com.rackspacecloud.blueflood.outputs.serializers.BatchedMetricsOutputSerializer;
import com.rackspacecloud.blueflood.outputs.utils.PlotRequestParser;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.HttpConfig;
import com.rackspacecloud.blueflood.types.BatchMetricsQuery;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.RollupsQueryParams;
import com.rackspacecloud.blueflood.utils.TimeValue;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.*;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class HttpMultiRollupsQueryHandler implements HttpRequestHandler {
    private final BatchedMetricsOutputSerializer<JSONObject> serializer;
    private final Gson gson;           // thread-safe
    private final JsonParser parser;   // thread-safe
    private final Timer httpBatchMetricsFetchTimer = Metrics.newTimer(HttpMultiRollupsQueryHandler.class,
            "Handle HTTP batch request for metrics", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private final ThreadPoolExecutor executor;
    private final TimeValue queryTimeout;
    private final int maxMetricsPerRequest;

    public HttpMultiRollupsQueryHandler() {
        Configuration config = Configuration.getInstance();
        int maxThreadsToUse = config.getIntegerProperty(HttpConfig.MAX_READ_WORKER_THREADS);
        int maxQueueSize = config.getIntegerProperty(HttpConfig.MAX_BATCH_READ_REQUESTS_TO_QUEUE);
        this.queryTimeout = new TimeValue(
                config.getIntegerProperty(HttpConfig.BATCH_QUERY_TIMEOUT),
                TimeUnit.SECONDS
        );
        this.maxMetricsPerRequest = config.getIntegerProperty(HttpConfig.MAX_METRICS_PER_BATCH_QUERY);
        this.serializer = new BatchedMetricsJSONOutputSerializer();
        this.gson = new GsonBuilder().setPrettyPrinting().serializeNulls().create();
        this.parser = new JsonParser();
        this.executor = new ThreadPoolBuilder().withCorePoolSize(maxThreadsToUse).withMaxPoolSize(maxThreadsToUse)
                .withName("HTTP-BatchMetricsFetch").withBoundedQueue(maxQueueSize).build();
    }

    @Override
    public void handle(ChannelHandlerContext ctx, HttpRequest request) {
        final String tenantId = request.getHeader("tenantId");

        if (!(request instanceof HTTPRequestWithDecodedQueryParams)) {
            sendResponse(ctx, request, "Missing query params: from, to, points",
                    HttpResponseStatus.BAD_REQUEST);
            return;
        }

        final String body = request.getContent().toString(Constants.DEFAULT_CHARSET);

        if (body == null || body.isEmpty()) {
            sendResponse(ctx, request, "Invalid body. Expected JSON array of metrics.",
                    HttpResponseStatus.BAD_REQUEST);
            return;
        }

        List<Locator> locators;
        try {
            locators = getLocatorsFromJSONBody(tenantId, body);
        } catch (Exception ex) {
            sendResponse(ctx, request, ex.getMessage(), HttpResponseStatus.BAD_REQUEST);
            return;
        }

        if (locators.size() > maxMetricsPerRequest) {
            sendResponse(ctx, request, "Too many metrics fetch in a single call. Max limit is " + maxMetricsPerRequest
                    + ".", HttpResponseStatus.BAD_REQUEST);
            return;
        }

        HTTPRequestWithDecodedQueryParams requestWithParams = (HTTPRequestWithDecodedQueryParams) request;

        final TimerContext httpBatchMetricsFetchTimerContext = httpBatchMetricsFetchTimer.time();
        try {
            RollupsQueryParams params = PlotRequestParser.parseParams(requestWithParams.getQueryParams());
            BatchMetricsQuery query = new BatchMetricsQuery(locators, params.getRange(), params.getGranularity());
            Map<Locator, MetricData> results = new BatchMetricsQueryHandler(executor, AstyanaxReader.getInstance())
                                                        .execute(query, queryTimeout);
            JSONObject metrics = serializer.transformRollupData(results, params.getStats());
            final JsonElement element = parser.parse(metrics.toString());
            final String jsonStringRep = gson.toJson(element);
            sendResponse(ctx, request, jsonStringRep, HttpResponseStatus.OK);
        } catch (InvalidRequestException e) {
            sendResponse(ctx, request, e.getMessage(), HttpResponseStatus.BAD_REQUEST);
        } catch (SerializationException e) {
            sendResponse(ctx, request, e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception e) {
            sendResponse(ctx, request, e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } finally {
            httpBatchMetricsFetchTimerContext.stop();
        }
    }

    private List<Locator> getLocatorsFromJSONBody(String tenantId, String body) {
        JsonElement element = gson.fromJson(body, JsonElement.class);
        JsonArray metrics = element.getAsJsonArray();
        final List<Locator> locators = new ArrayList<Locator>();

        Iterator<JsonElement> it = metrics.iterator();
        while (it.hasNext()) {
            JsonElement metricElement = it.next();
            Locator loc = Locator.createLocatorFromPathComponents(tenantId, metricElement.getAsString());
            locators.add(loc);
        }

        return locators;
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
