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
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.rackspacecloud.blueflood.http.DefaultHandler;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.inputs.formats.AggregatedPayload;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.tracker.Tracker;
import com.rackspacecloud.blueflood.types.MetricsCollection;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
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

    public HttpAggregatedMultiIngestionHandler(HttpMetricsIngestionServer.Processor processor, TimeValue timeout) {
        this.processor = processor;
        this.timeout = timeout;
    }

    // our own stuff.
    @Override
    public void handle(ChannelHandlerContext ctx, HttpRequest request) {

        Tracker.getInstance().track(request);

        final Timer.Context timerContext = handlerTimer.time();

        // this is all JSON.
        final String body = request.getContent().toString(Constants.DEFAULT_CHARSET);
        try {
            // block until things get ingested.
            requestCount.inc();
            List<AggregatedPayload> bundleList = createBundleList(body);

            if (bundleList.size() > 0) {
                // has aggregated metric bundle in body
                // convert and add metric bundle to MetricsCollection if valid
                MetricsCollection collection = new MetricsCollection();
                List<String> errors = new ArrayList<String>();

                // for each metric bundle
                for (AggregatedPayload bundle : bundleList) {
                    // validate, convert, and add to collection
                    List<String> bundleValidationErrors = bundle.getValidationErrors();
                    if (bundleValidationErrors.isEmpty()) {
                        // no validation error, add to collection
                        collection.add(PreaggregateConversions.buildMetricsCollection(bundle));
                    }
                    else {
                        // failed validation, add to error
                        errors.addAll( bundleValidationErrors );
                    }
                }

                // if has validation errors and no valid metrics
                if (!errors.isEmpty() && collection.size() == 0) {
                    // return BAD_REQUEST and error
                    DefaultHandler.sendResponse( ctx, request,
                            HttpMetricsIngestionHandler.getResponseBody(errors),
                            HttpResponseStatus.BAD_REQUEST );
                    return;
                }

                // process valid metrics in collection
                ListenableFuture<List<Boolean>> futures = processor.apply(collection);
                List<Boolean> persisteds = futures.get(timeout.getValue(), timeout.getUnit());
                for (Boolean persisted : persisteds) {
                    if (!persisted) {
                        DefaultHandler.sendResponse(ctx, request, null, HttpResponseStatus.INTERNAL_SERVER_ERROR);
                        return;
                    }
                }

                // return OK or MULTI_STATUS response depending if there were validation errors
                if (errors.isEmpty()) {
                    // no validation error, response OK
                    DefaultHandler.sendResponse(ctx, request, null, HttpResponseStatus.OK);
                }
                else {
                    // has some validation errors, response MULTI_STATUS
                    DefaultHandler.sendResponse(ctx, request, null, HttpResponseStatus.MULTI_STATUS);
                }

            }
            else {
                // no aggregated metric bundles in body
                DefaultHandler.sendResponse( ctx, request, null, HttpResponseStatus.BAD_REQUEST );
            }
        } catch (JsonParseException ex) {
            log.debug(String.format("BAD JSON: %s", body));
            log.error(ex.getMessage(), ex);
            DefaultHandler.sendResponse(ctx, request, ex.getMessage(), HttpResponseStatus.BAD_REQUEST);
        } catch (ConnectionException ex) {
            log.error(ex.getMessage(), ex);
            DefaultHandler.sendResponse(ctx, request, "Internal error saving data", HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } catch (TimeoutException ex) {
            DefaultHandler.sendResponse(ctx, request, "Timed out persisting metrics", HttpResponseStatus.ACCEPTED);
        } catch (Exception ex) {
            log.debug(String.format("BAD JSON: %s", body));
            log.error("Other exception while trying to parse content", ex);
            DefaultHandler.sendResponse(ctx, request, "Failed parsing content", HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } finally {
            requestCount.dec();
            timerContext.stop();
        }
    }

    public static List<AggregatedPayload> createBundleList(String json) {
        Gson gson = new Gson();
        JsonParser parser = new JsonParser();
        JsonArray jArray = parser.parse(json).getAsJsonArray();

        ArrayList<AggregatedPayload> bundleList = new ArrayList<AggregatedPayload>();

        for(JsonElement obj : jArray )
        {
            AggregatedPayload bundle = gson.fromJson( obj , AggregatedPayload.class);
            bundleList.add(bundle);
        }

        return bundleList;
    }
}
