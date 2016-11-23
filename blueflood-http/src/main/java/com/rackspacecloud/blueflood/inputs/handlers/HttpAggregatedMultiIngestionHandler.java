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
import com.google.gson.*;
import com.rackspacecloud.blueflood.exceptions.InvalidDataException;
import com.rackspacecloud.blueflood.http.DefaultHandler;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.inputs.formats.AggregatedPayload;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.outputs.formats.ErrorResponse;
import com.rackspacecloud.blueflood.tracker.Tracker;
import com.rackspacecloud.blueflood.types.MetricsCollection;
import com.rackspacecloud.blueflood.utils.Clock;
import com.rackspacecloud.blueflood.utils.DefaultClockImpl;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.TimeValue;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class HttpAggregatedMultiIngestionHandler implements HttpRequestHandler {

    private static final Logger log = LoggerFactory.getLogger(HttpAggregatedMultiIngestionHandler.class);

    private static final Timer handlerTimer = Metrics.timer(HttpAggregatedMultiIngestionHandler.class, "HTTP aggregated multi metrics ingestion timer");
    private static final Counter requestCount = Metrics.counter(HttpAggregatedMultiIngestionHandler.class, "HTTP aggregated multi Request Count");

    private final HttpMetricsIngestionServer.Processor processor;
    private final TimeValue timeout;
    private final Clock clock = new DefaultClockImpl();

    public HttpAggregatedMultiIngestionHandler(HttpMetricsIngestionServer.Processor processor, TimeValue timeout) {
        this.processor = processor;
        this.timeout = timeout;
    }

    // our own stuff.
    @Override
    public void handle(ChannelHandlerContext ctx, FullHttpRequest request) {

        Tracker.getInstance().track(request);

        requestCount.inc();

        final Timer.Context timerContext = handlerTimer.time();
        long ingestTime = clock.now().getMillis();

        // this is all JSON.
        String body = null;
        try {
            body = request.content().toString(Constants.DEFAULT_CHARSET);
            List<AggregatedPayload> bundleList = createBundleList(body);

            if (bundleList.size() > 0) {
                // has aggregated metric bundle in body
                // convert and add metric bundle to MetricsCollection if valid
                MetricsCollection collection = new MetricsCollection();
                List<ErrorResponse.ErrorData> errors = new ArrayList<ErrorResponse.ErrorData>();

                // for each metric bundle
                for (AggregatedPayload bundle : bundleList) {
                    // validate, convert, and add to collection
                    List<ErrorResponse.ErrorData> bundleValidationErrors = bundle.getValidationErrors();
                    if (bundleValidationErrors.isEmpty()) {
                        // no validation error, add to collection
                        collection.add(PreaggregateConversions.buildMetricsCollection(bundle));
                    } else {
                        // failed validation, add to error
                        errors.addAll(bundleValidationErrors);
                    }

                    if (bundle.hasDelayedMetrics(ingestTime)) {
                        Tracker.getInstance().trackDelayedAggregatedMetricsTenant(bundle.getTenantId(),
                                bundle.getTimestamp(),
                                bundle.getDelayTime(ingestTime),
                                bundle.getAllMetricNames());
                        bundle.markDelayMetricsReceived(ingestTime);
                    }
                }

                // if has validation errors and no valid metrics
                if (!errors.isEmpty() && collection.size() == 0) {
                    // return BAD_REQUEST and error
                    DefaultHandler.sendErrorResponse(ctx, request,
                            errors,
                            HttpResponseStatus.BAD_REQUEST);
                    return;
                }

                // process valid metrics in collection
                ListenableFuture<List<Boolean>> futures = processor.apply(collection);
                List<Boolean> persisteds = futures.get(timeout.getValue(), timeout.getUnit());
                for (Boolean persisted : persisteds) {
                    if (!persisted) {
                        DefaultHandler.sendErrorResponse(ctx, request, "Internal error persisting data", HttpResponseStatus.INTERNAL_SERVER_ERROR);
                        return;
                    }
                }

                // return OK or MULTI_STATUS response depending if there were validation errors
                if (errors.isEmpty()) {
                    // no validation error, response OK
                    DefaultHandler.sendResponse(ctx, request, null, HttpResponseStatus.OK);
                    return;
                } else {
                    // has some validation errors, response MULTI_STATUS
                    DefaultHandler.sendErrorResponse(ctx, request, errors, HttpResponseStatus.MULTI_STATUS);
                    return;
                }

            } else {
                // no aggregated metric bundles in body, response OK
                DefaultHandler.sendResponse(ctx, request, "No valid metrics", HttpResponseStatus.BAD_REQUEST);
                return;
            }
        } catch (JsonParseException ex) {
            log.debug(String.format("BAD JSON: %s", body));
            log.error(ex.getMessage(), ex);
            DefaultHandler.sendErrorResponse(ctx, request, ex.getMessage(), HttpResponseStatus.BAD_REQUEST);
        } catch (InvalidDataException ex) {
            log.debug(String.format("Invalid request body: %s", body));
            log.error(ex.getMessage(), ex);
            DefaultHandler.sendErrorResponse(ctx, request, ex.getMessage(), HttpResponseStatus.BAD_REQUEST);
        } catch (TimeoutException ex) {
            DefaultHandler.sendErrorResponse(ctx, request, "Timed out persisting metrics", HttpResponseStatus.ACCEPTED);
        } catch (Exception ex) {
            log.debug(String.format("BAD JSON: %s", body));
            log.error("Other exception while trying to parse content", ex);
            DefaultHandler.sendErrorResponse(ctx, request, "Internal error saving data", HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } finally {
            timerContext.stop();
            requestCount.dec();
        }

    }

    public static List<AggregatedPayload> createBundleList(String json) {
        Gson gson = new Gson();
        JsonParser parser = new JsonParser();
        JsonElement element = parser.parse(json);

        if (!element.isJsonArray()) {
            throw new InvalidDataException("Invalid request body");
        }

        JsonArray jArray = element.getAsJsonArray();

        ArrayList<AggregatedPayload> bundleList = new ArrayList<AggregatedPayload>();

        for(JsonElement obj : jArray )
        {
            AggregatedPayload bundle = AggregatedPayload.create(obj);
            bundleList.add(bundle);
        }

        return bundleList;
    }
}
