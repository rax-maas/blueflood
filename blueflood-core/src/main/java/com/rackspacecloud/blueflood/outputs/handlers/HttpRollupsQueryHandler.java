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

import java.util.*;
import java.util.concurrent.TimeUnit;

public class HttpRollupsQueryHandler extends RollupHandler
            implements MetricDataQueryInterface<MetricData>, HttpRequestHandler {
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
        defaultStats.add("numPoints");
    }

    private JSONObject GetDataByPoints(String tenantId,
                                      String metric,
                                      long from,
                                      long to,
                                      int points,
                                      Set<String> stats) throws SerializationException {
        return serializer.transformRollupData(GetDataByPoints(tenantId, metric, from, to, points), stats);
    }

    private JSONObject GetDataByResolution(String tenantId,
                                      String metric,
                                      long from,
                                      long to,
                                      Resolution resolution,
                                      Set<String> stats) throws SerializationException {
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
            RollupsQueryParams params = parseParams(requestWithParams.getQueryParams());

            JSONObject metricData;
            if (params.isPoints) {
                metricData = GetDataByPoints(tenantId, metricName, params.from, params.to, params.points, params.stats);
            } else if (params.isResolution) {
                metricData = GetDataByResolution(tenantId, metricName, params.from, params.to, params.resolution,
                        params.stats);
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
        List<String> select = params.get("select");

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

        Set<String> stats = getStatsToFilter(select);

        if (points != null) {
            return new RollupsQueryParams(fromTime, toTime, Integer.parseInt(points.get(0)), stats);
        } else {
            return new RollupsQueryParams(fromTime, toTime, Resolution.fromString(res.get(0)), stats);
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

    private Set<String> getStatsToFilter(List<String> select) {
        if (select == null || select.isEmpty()) {
            return defaultStats;
        } else {
            Set<String> filters = new HashSet<String>();
            // handle case when someone does select=average,min instead of select=average&select=min
            for (String stat : select) {
                if (stat.contains(",")) {
                    List<String> nestedStats = Arrays.asList(stat.split(","));
                    filters.addAll(nestedStats);
                }
            }
            return filters;
        }
    }

    private class RollupsQueryParams {
        public int points;
        public Resolution resolution;
        public final long from;
        public final long to;
        private final Set<String> stats;
        public boolean isPoints = true;
        public boolean isResolution = false;

        private RollupsQueryParams(long from, long to, Set<String> stats) {
            this.stats = stats;
            this.from = from;
            this.to = to;
        }

        public RollupsQueryParams(long from, long to, int points, Set<String> stats) {
            this(from, to, stats);
            this.isResolution = false;
            this.isPoints = true;
            this.points = points;
        }

        public RollupsQueryParams(long from, long to, Resolution resolution, Set<String> stats) {
            this(from, to, stats);
            this.resolution = resolution;
            this.isPoints = false;
            this.isResolution = true;
        }
    }
}
