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

package com.rackspacecloud.blueflood.inputs.handlers;

import com.codahale.metrics.Timer;
import com.rackspacecloud.blueflood.concurrent.AsyncChain;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.http.HttpResponder;
import com.rackspacecloud.blueflood.inputs.formats.JSONMetricsContainer;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.MetricsCollection;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class HttpMetricsIngestionHandler implements HttpRequestHandler {
    private static final Logger log = LoggerFactory.getLogger(HttpMetricsIngestionHandler.class);

    private final ObjectMapper mapper;
    private final TypeFactory typeFactory;
    private final AsyncChain<MetricsCollection, Boolean> processorChain;
    private final TimeValue timeout;

    // Metrics
    private static final Timer handlerTimer = Metrics.timer(HttpMetricsIngestionHandler.class, "HTTP metrics ingestion timer");

    public HttpMetricsIngestionHandler(AsyncChain<MetricsCollection, Boolean> processorChain, TimeValue timeout) {
        this.mapper = new ObjectMapper();
        this.typeFactory = TypeFactory.defaultInstance();
        this.timeout = timeout;
        this.processorChain = processorChain;
    }

    @Override
    public void handle(ChannelHandlerContext ctx, HttpRequest request) {
        final String tenantId = request.getHeader("tenantId");
        JSONMetricsContainer jsonMetricsContainer = null;

        final Timer.Context timerContext = handlerTimer.time();
        final String body = request.getContent().toString(Constants.DEFAULT_CHARSET);
        try {
            List<JSONMetricsContainer.JSONMetric> jsonMetrics =
                    mapper.readValue(
                            body,
                            typeFactory.constructCollectionType(List.class,
                                    JSONMetricsContainer.JSONMetric.class)
                    );
            jsonMetricsContainer = new JSONMetricsContainer(tenantId, jsonMetrics);
        } catch (JsonParseException e) {
            log.warn("Exception parsing content", e);
            sendResponse(ctx, request, "Cannot parse content", HttpResponseStatus.BAD_REQUEST);
            return;
        } catch (JsonMappingException e) {
            log.warn("Exception parsing content", e);
            sendResponse(ctx, request, "Cannot parse content", HttpResponseStatus.BAD_REQUEST);
            return;
        } catch (IOException e) {
            log.warn("IO Exception parsing content", e);
            sendResponse(ctx, request, "Cannot parse content", HttpResponseStatus.BAD_REQUEST);
            return;
        } catch (Exception e) {
            log.warn("Other exception while trying to parse content", e);
            sendResponse(ctx, request, "Failed parsing content", HttpResponseStatus.INTERNAL_SERVER_ERROR);
            return;
        }

        if (jsonMetricsContainer == null) {
            sendResponse(ctx, request, null, HttpResponseStatus.OK);
            return;
        }

        List<Metric> containerMetrics = jsonMetricsContainer.toMetrics();
        if (containerMetrics == null || containerMetrics.isEmpty()) {
            sendResponse(ctx, request, null, HttpResponseStatus.OK);
            return;
        }

        final MetricsCollection collection = new MetricsCollection();
        collection.add(new ArrayList<IMetric>(containerMetrics));

        try {
            processorChain.apply(collection).get(timeout.getValue(), timeout.getUnit());
            sendResponse(ctx, request, null, HttpResponseStatus.OK);
        } catch (TimeoutException e) {
            sendResponse(ctx, request, "Timed out persisting metrics", HttpResponseStatus.ACCEPTED);
        } catch (Exception e) {
            log.error("Exception persisting metrics", e);
            sendResponse(ctx, request, "Error persisting metrics", HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } finally {
            timerContext.stop();
        }
    }

    public static void sendResponse(ChannelHandlerContext channel, HttpRequest request, String messageBody, HttpResponseStatus status) {
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);

        if (messageBody != null && !messageBody.isEmpty()) {
            response.setContent(ChannelBuffers.copiedBuffer(messageBody, Constants.DEFAULT_CHARSET));
        }
        HttpResponder.respond(channel, request, response);
    }
}
