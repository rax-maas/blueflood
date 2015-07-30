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

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
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
import com.rackspacecloud.blueflood.outputs.serializers.JSONHistogramOutputSerializer;
import com.rackspacecloud.blueflood.outputs.utils.PlotRequestParser;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.tracker.Tracker;
import com.rackspacecloud.blueflood.types.Resolution;
import com.rackspacecloud.blueflood.outputs.utils.RollupsQueryParams;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.*;
import org.json.simple.JSONObject;

import java.io.IOException;

public class HttpHistogramQueryHandler extends RollupHandler implements HttpRequestHandler {
    private final JSONHistogramOutputSerializer serializer;
    private final Gson gson;           // thread-safe
    private final JsonParser parser;   // thread-safe

    private static final Timer histFetchTimer = Metrics.timer(HttpRollupsQueryHandler.class,
            "Handle HTTP request for histograms");
    private static final Meter histByPointsMeter = Metrics.meter(RollupHandler.class, "Get histograms by points",
            "BF-API");
    private static final Meter histByGranularityMeter = Metrics.meter(RollupHandler.class, "Get histograms by gran",
            "BF-API");

    public HttpHistogramQueryHandler() {
        this.gson = new GsonBuilder().setPrettyPrinting().serializeNulls().create();
        this.parser = new JsonParser();
        this.serializer = new JSONHistogramOutputSerializer();
    }

    private JSONObject GetHistogramByPoints(String tenantId,
                                       String metric,
                                       long from,
                                       long to,
                                       int points) throws IOException, SerializationException {
        histByPointsMeter.mark();
        Granularity g = Granularity.granularityFromPointsInInterval(tenantId,from, to, points);
        return serializer.transformHistogram(getHistogramsByGranularity(tenantId, metric, from, to, g));
    }

    private JSONObject GetHistogramByResolution(String tenantId,
                                            String metric,
                                            long from,
                                            long to,
                                            Resolution resolution) throws IOException, SerializationException {
        histByGranularityMeter.mark();
        if (resolution == null || resolution == Resolution.FULL) {
            resolution = Resolution.MIN5;
        }
        Granularity g = Granularity.granularities()[resolution.getValue()];
        return serializer.transformHistogram(getHistogramsByGranularity(tenantId, metric, from, to, g));
    }

    @Override
    public void handle(ChannelHandlerContext ctx, HttpRequest request) {

        Tracker.track(request);

        final String tenantId = request.getHeader("tenantId");
        final String metricName = request.getHeader("metricName");

        if (!(request instanceof HTTPRequestWithDecodedQueryParams)) {
            sendResponse(ctx, request, "Missing query params: from, to, points",
                    HttpResponseStatus.BAD_REQUEST);
            return;
        }

        HTTPRequestWithDecodedQueryParams requestWithParams = (HTTPRequestWithDecodedQueryParams) request;
        final Timer.Context histFetchTimerContext = histFetchTimer.time();

        try {
            RollupsQueryParams params = PlotRequestParser.parseParams(requestWithParams.getQueryParams());

            JSONObject metricData;
            if (params.isGetByPoints()) {
                metricData = GetHistogramByPoints(tenantId, metricName, params.getRange().getStart(),
                        params.getRange().getStop(), params.getPoints());
            } else if (params.isGetByResolution()) {
                metricData = GetHistogramByResolution(tenantId, metricName, params.getRange().getStart(),
                        params.getRange().getStop(), params.getResolution());
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
            histFetchTimerContext.stop();
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
}
