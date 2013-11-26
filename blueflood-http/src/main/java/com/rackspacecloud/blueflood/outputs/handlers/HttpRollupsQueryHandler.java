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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.rackspacecloud.blueflood.exceptions.InvalidRequestException;
import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.http.HTTPRequestWithDecodedQueryParams;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.http.HttpResponder;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.outputs.serializers.JSONOutputSerializer;
import com.rackspacecloud.blueflood.outputs.serializers.OutputSerializer;
import com.rackspacecloud.blueflood.outputs.serializers.OutputSerializer.MetricStat;
import com.rackspacecloud.blueflood.outputs.utils.PlotRequestParser;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.Resolution;
import com.rackspacecloud.blueflood.types.RollupsQueryParams;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.*;
import org.json.simple.JSONObject;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class HttpRollupsQueryHandler extends RollupHandler
            implements MetricDataQueryInterface<MetricData>, HttpRequestHandler {
    private final OutputSerializer<JSONObject> serializer;
    private final Gson gson;           // thread-safe
    private final JsonParser parser;   // thread-safe
    private final Timer httpMetricsFetchTimer = Metrics.newTimer(HttpRollupsQueryHandler.class,
            "Handle HTTP request for metrics", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

    public HttpRollupsQueryHandler() {
        this.serializer = new JSONOutputSerializer();
        this.gson = new GsonBuilder().setPrettyPrinting().serializeNulls().create();
        this.parser = new JsonParser();
    }

    private JSONObject GetDataByPoints(String tenantId,
                                      String metric,
                                      long from,
                                      long to,
                                      int points,
                                      Set<MetricStat> stats) throws SerializationException {
        return serializer.transformRollupData(GetDataByPoints(tenantId, metric, from, to, points), stats);
    }

    private JSONObject GetDataByResolution(String tenantId,
                                      String metric,
                                      long from,
                                      long to,
                                      Resolution resolution,
                                      Set<MetricStat> stats) throws SerializationException {
        return serializer.transformRollupData(GetDataByResolution(tenantId, metric, from, to, resolution), stats);
    }

    @Override
    public MetricData GetDataByPoints(String tenantId,
                                      String metric,
                                      long from,
                                      long to,
                                      int points) throws SerializationException {
        rollupsByPointsMeter.mark();
        Granularity g = Granularity.granularityFromPointsInInterval(from, to, points);
        return getRollupByGranularity(tenantId, metric, from, to, g);
    }

    @Override
    public MetricData GetDataByResolution(String tenantId,
                                          String metric,
                                          long from,
                                          long to,
                                          Resolution resolution) throws SerializationException {
        rollupsByGranularityMeter.mark();
        if (resolution == null) {
            resolution = Resolution.FULL;
        }
        Granularity g = Granularity.granularities()[resolution.getValue()];
        return getRollupByGranularity(tenantId, metric, from, to, g);
    }

    @Override
    public void handle(ChannelHandlerContext ctx, HttpRequest request) {
        final String tenantId = request.getHeader("tenantId");
        final String metricName = request.getHeader("metricName");

        if (!(request instanceof HTTPRequestWithDecodedQueryParams)) {
            sendResponse(ctx, request, "Missing query params: from, to, points",
                    HttpResponseStatus.BAD_REQUEST);
            return;
        }

        HTTPRequestWithDecodedQueryParams requestWithParams = (HTTPRequestWithDecodedQueryParams) request;

        final TimerContext httpMetricsFetchTimerContext = httpMetricsFetchTimer.time();
        try {
            RollupsQueryParams params = PlotRequestParser.parseParams(requestWithParams.getQueryParams());

            JSONObject metricData;
            if (params.isGetByPoints()) {
                metricData = GetDataByPoints(tenantId, metricName, params.getRange().getStart(),
                        params.getRange().getStop(), params.getPoints(), params.getStats());
            } else if (params.isGetByResolution()) {
                metricData = GetDataByResolution(tenantId, metricName, params.getRange().getStart(),
                        params.getRange().getStop(), params.getResolution(), params.getStats());
            } else {
                throw new InvalidRequestException("Invalid rollups query. Neither points nor resolution specified.");
            }

            final JsonElement element = parser.parse(metricData.toString());
            final String jsonStringRep = gson.toJson(element);
            sendResponse(ctx, request, jsonStringRep, HttpResponseStatus.OK);
        } catch (InvalidRequestException e) {
            sendResponse(ctx, request, e.getMessage(), HttpResponseStatus.BAD_REQUEST);
        } catch (SerializationException e) {
            sendResponse(ctx, request, e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception e) {
            sendResponse(ctx, request, e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } finally {
            httpMetricsFetchTimerContext.stop();
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
