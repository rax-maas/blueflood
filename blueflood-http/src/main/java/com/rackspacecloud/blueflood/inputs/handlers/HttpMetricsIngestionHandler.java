/*
 * Copyright 2013-2015 Rackspace
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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.cache.ConfigTtlProvider;
import com.rackspacecloud.blueflood.exceptions.InvalidDataException;
import com.rackspacecloud.blueflood.http.DefaultHandler;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.http.HttpResponder;
import com.rackspacecloud.blueflood.inputs.formats.JSONMetricsContainer;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.tracker.Tracker;
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
    private static final Counter requestCount = Metrics.counter(HttpMetricsIngestionHandler.class, "HTTP Request Count");


    protected final ObjectMapper mapper;
    protected final TypeFactory typeFactory;
    private final HttpMetricsIngestionServer.Processor processor;
    private final TimeValue timeout;

    // Metrics
    private static final Timer jsonTimer = Metrics.timer(HttpMetricsIngestionHandler.class, "HTTP Ingestion json processing timer");
    private static final Timer persistingTimer = Metrics.timer(HttpMetricsIngestionHandler.class, "HTTP Ingestion persisting timer");

    public HttpMetricsIngestionHandler(HttpMetricsIngestionServer.Processor processor, TimeValue timeout) {
        this.mapper = new ObjectMapper();
        this.typeFactory = TypeFactory.defaultInstance();
        this.timeout = timeout;
        this.processor = processor;
    }

    protected JSONMetricsContainer createContainer(String body, String tenantId) throws JsonParseException, JsonMappingException, IOException {
        List<JSONMetricsContainer.JSONMetric> jsonMetrics =
                mapper.readValue(
                        body,
                        typeFactory.constructCollectionType(List.class,
                                JSONMetricsContainer.JSONMetric.class)
                );
        return new JSONMetricsContainer(tenantId, jsonMetrics);
    }

    @Override
    public void handle(ChannelHandlerContext ctx, HttpRequest request) {
        try {

            Tracker.track(request);

            requestCount.inc();
            final String tenantId = request.getHeader("tenantId");
            JSONMetricsContainer jsonMetricsContainer = null;
            final Timer.Context jsonTimerContext = jsonTimer.time();

            final String body = request.getContent().toString(Constants.DEFAULT_CHARSET);
            try {
                jsonMetricsContainer = createContainer(body, tenantId);
                if (!jsonMetricsContainer.isValid()) {
                    throw new IOException("Invalid JSONMetricsContainer");
                }
            } catch (JsonParseException e) {
                log.warn("Exception parsing content", e);
                DefaultHandler.sendResponse(ctx, request, "Cannot parse content", HttpResponseStatus.BAD_REQUEST);
                return;
            } catch (JsonMappingException e) {
                log.warn("Exception parsing content", e);
                DefaultHandler.sendResponse(ctx, request, "Cannot parse content", HttpResponseStatus.BAD_REQUEST);
                return;
            } catch (IOException e) {
                log.warn("IO Exception parsing content", e);
                DefaultHandler.sendResponse(ctx, request, "Cannot parse content", HttpResponseStatus.BAD_REQUEST);
                return;
            } catch (Exception e) {
                log.warn("Other exception while trying to parse content", e);
                DefaultHandler.sendResponse(ctx, request, "Failed parsing content", HttpResponseStatus.INTERNAL_SERVER_ERROR);
                return;
            }

            if (jsonMetricsContainer == null) {
                log.warn(ctx.getChannel().getRemoteAddress() + " No valid metrics");
                DefaultHandler.sendResponse(ctx, request, "No valid metrics", HttpResponseStatus.BAD_REQUEST);
                return;
            }

            List<Metric> containerMetrics;
            try {
                containerMetrics = jsonMetricsContainer.toMetrics();
                forceTTLsIfConfigured(containerMetrics);
            } catch (InvalidDataException ex) {
                // todo: we should measure these. if they spike, we track down the bad client.
                // this is strictly a client problem. Someting wasn't right (data out of range, etc.)
                log.warn(ctx.getChannel().getRemoteAddress() + " " + ex.getMessage());
                DefaultHandler.sendResponse(ctx, request, "Invalid data " + ex.getMessage(), HttpResponseStatus.BAD_REQUEST);
                return;
            } catch (Exception e) {
                // todo: when you see these in logs, go and fix them (throw InvalidDataExceptions) so they can be reduced
                // to single-line log statements.
                log.warn("Exception converting JSON container to metric objects", e);
                // This could happen if clients send BigIntegers as metric values. BF doesn't handle them. So let's send a
                // BAD REQUEST message until we start handling BigIntegers.
                DefaultHandler.sendResponse(ctx, request, "Error converting JSON payload to metric objects",
                        HttpResponseStatus.BAD_REQUEST);
                return;
            } finally {
                jsonTimerContext.stop();
            }

            if (containerMetrics == null || containerMetrics.isEmpty()) {
                log.warn(ctx.getChannel().getRemoteAddress() + " No valid metrics");
                DefaultHandler.sendResponse(ctx, request, "No valid metrics", HttpResponseStatus.BAD_REQUEST);
            }

            final MetricsCollection collection = new MetricsCollection();
            collection.add(new ArrayList<IMetric>(containerMetrics));
            final Timer.Context persistingTimerContext = persistingTimer.time();
            try {
                ListenableFuture<List<Boolean>> futures = processor.apply(collection);
                List<Boolean> persisteds = futures.get(timeout.getValue(), timeout.getUnit());
                for (Boolean persisted : persisteds) {
                    if (!persisted) {
                        DefaultHandler.sendResponse(ctx, request, null, HttpResponseStatus.INTERNAL_SERVER_ERROR);
                        return;
                    }
                }
                DefaultHandler.sendResponse(ctx, request, null, HttpResponseStatus.OK);
            } catch (TimeoutException e) {
                DefaultHandler.sendResponse(ctx, request, "Timed out persisting metrics", HttpResponseStatus.ACCEPTED);
            } catch (Exception e) {
                log.error("Exception persisting metrics", e);
                DefaultHandler.sendResponse(ctx, request, "Error persisting metrics", HttpResponseStatus.INTERNAL_SERVER_ERROR);
            } finally {
                persistingTimerContext.stop();
            }
        } finally {
            requestCount.dec();
        }
    }

    private void forceTTLsIfConfigured(List<Metric> containerMetrics) {
        ConfigTtlProvider configTtlProvider = ConfigTtlProvider.getInstance();

        if(configTtlProvider.areTTLsForced()) {
            for(Metric m : containerMetrics) {
                m.setTtl(configTtlProvider.getConfigTTLForIngestion());
            }
        }
    }
}
