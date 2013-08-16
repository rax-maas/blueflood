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
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.Resolution;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.*;
import org.json.simple.JSONObject;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class HttpRollupsQueryHandler extends RollupHandler
            implements MetricDataQueryInterface<JSONObject>, HttpRequestHandler {
    private final OutputSerializer<JSONObject> serializer;
    private final Gson gson;           // thread-safe
    private final JsonParser parser;   // thread-safe
    private final Set<String> defaultStats;

    private final Timer httpMetricsFetchTimer = Metrics.newTimer(HttpRollupsQueryHandler.class,
            "Handle HTTP request for metrics", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

    public HttpRollupsQueryHandler() {
        this.serializer = new JSONOutputSerializer();
        gson = new GsonBuilder().setPrettyPrinting().serializeNulls().create();
        parser = new JsonParser();
        defaultStats = new HashSet<String>();
        defaultStats.add("average");
    }

    @Override
    public JSONObject GetDataByPoints(String tenantId,
                                      String metric,
                                      long from,
                                      long to,
                                      int points) throws SerializationException {
        rollupsByPointsMeter.mark();
        Granularity g = Granularity.granularityFromPointsInInterval(from, to, points);
        MetricData data = getRollupByGranularity(tenantId, metric, from, to, g);
        return serializer.transformRollupData(data, defaultStats);
    }

    @Override
    public JSONObject GetDataByResolution(String tenantId,
                                          String metric,
                                          long from,
                                          long to,
                                          Resolution resolution) throws SerializationException {
        rollupsByGranularityMeter.mark();
        if (resolution == null) {
            resolution = Resolution.FULL;
        }
        Granularity g = Granularity.granularities()[resolution.getValue()];
        MetricData data = getRollupByGranularity(tenantId, metric, from, to, g);
        return serializer.transformRollupData(data, defaultStats);
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
            RollupsQueryParams params = parseParams(requestWithParams.getQueryParams());

            JSONObject metricData;
            if (params.isPoints) {
                metricData = GetDataByPoints(tenantId, metricName, params.from, params.to, params.points);
            } else if (params.isResolution) {
                metricData = GetDataByResolution(tenantId, metricName, params.from, params.to, params.resolution);
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

    public RollupsQueryParams parseParams(Map<String, List<String>> params) throws InvalidRequestException {
        if (params == null || params.isEmpty()) {
            throw new InvalidRequestException("No query parameters present.");
        }

        List<String> points = params.get("points");
        List<String> res = params.get("resolution");
        List<String> from = params.get("from");
        List<String> to = params.get("to");

        if (points == null && res == null) {
            throw new InvalidRequestException("Either 'points' or 'resolution' is required.");
        }

        if (points != null && points.size() != 1) {
            throw new InvalidRequestException("Invalid parameter: points=" + points);
        } else if (res != null && res.size() != 1) {
            throw new InvalidRequestException("Invalid parameter: resolution=" + res);
        } else if (from == null || from.size() != 1) {
            throw new InvalidRequestException("Invalid parameter: from=" + from);
        } else if (to == null || to.size() != 1) {
            throw new InvalidRequestException("Invalid parameter: to="+ to);
        }

        long fromTime = Long.parseLong(from.get(0));
        long toTime = Long.parseLong(to.get(0));

        if (toTime <= fromTime) {
            throw new InvalidRequestException("paramter 'to' must be greater than 'from'");
        }

        if (points != null) {
            return new RollupsQueryParams(fromTime, toTime, Integer.parseInt(points.get(0)));
        } else {
            return new RollupsQueryParams(fromTime, toTime, Resolution.fromString(res.get(0)));
        }
    }

    public void sendResponse(ChannelHandlerContext channel, HttpRequest request, String messageBody,
                             HttpResponseStatus status) {
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);

        if (messageBody != null && !messageBody.isEmpty()) {
            response.setContent(ChannelBuffers.copiedBuffer(messageBody, Constants.DEFAULT_CHARSET));
        }
        HttpResponder.respond(channel, request, response);
    }

    private class RollupsQueryParams {
        public int points;
        public Resolution resolution;
        public long from;
        public long to;
        public boolean isPoints = true;
        public boolean isResolution = false;

        public RollupsQueryParams(long from, long to, int points) {
            this.from = from;
            this.to = to;
            this.points = points;
            this.isResolution = false;
            this.isPoints = true;
        }

        public RollupsQueryParams(long from, long to, Resolution resolution) {
            this.from = from;
            this.to = to;
            this.resolution = resolution;
            this.isPoints = false;
            this.isResolution = true;
        }
    }
}
