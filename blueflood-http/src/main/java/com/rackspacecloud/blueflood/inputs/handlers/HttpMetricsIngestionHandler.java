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
import com.rackspacecloud.blueflood.http.MediaTypeChecker;
import com.rackspacecloud.blueflood.inputs.formats.JSONMetric;
import com.rackspacecloud.blueflood.inputs.formats.JSONMetricsContainer;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.outputs.formats.ErrorResponse;
import com.rackspacecloud.blueflood.tracker.Tracker;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.MetricsCollection;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.TimeValue;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public class HttpMetricsIngestionHandler implements HttpRequestHandler {

    public static final String ERROR_HEADER = "The following errors have been encountered:";

    private static final Logger log = LoggerFactory.getLogger(HttpMetricsIngestionHandler.class);
    private static final Counter requestCount = Metrics.counter(HttpMetricsIngestionHandler.class, "HTTP Request Count");
    private static final MediaTypeChecker mediaTypeChecker = new MediaTypeChecker();

    protected final ObjectMapper mapper;
    protected final TypeFactory typeFactory;
    private final HttpMetricsIngestionServer.Processor processor;
    private final TimeValue timeout;

    private static final ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    protected static final Validator validator = factory.getValidator();

    // Metrics
    private static final Timer jsonTimer = Metrics.timer(HttpMetricsIngestionHandler.class, "HTTP Ingestion json processing timer");
    private static final Timer persistingTimer = Metrics.timer(HttpMetricsIngestionHandler.class, "HTTP Ingestion persisting timer");

    public static String getResponseBody( List<String> errors ) {
        StringBuilder sb = new StringBuilder();
        sb.append( ERROR_HEADER + System.lineSeparator() );

        for( String error : errors ) {

            sb.append( error + System.lineSeparator() );
        }
        return sb.toString();
    }

    public HttpMetricsIngestionHandler(HttpMetricsIngestionServer.Processor processor, TimeValue timeout) {
        this.mapper = new ObjectMapper();
        this.typeFactory = TypeFactory.defaultInstance();
        this.timeout = timeout;
        this.processor = processor;
    }

    protected JSONMetricsContainer createContainer(String body, String tenantId) throws JsonParseException, JsonMappingException, IOException {
        //mapping
        List<JSONMetric> jsonMetrics =
                mapper.readValue(
                        body,
                        typeFactory.constructCollectionType(List.class, JSONMetric.class)
                );

        //validation
        List<ErrorResponse.ErrorData> validationErrors = new ArrayList<ErrorResponse.ErrorData>();
        List<JSONMetric> validJsonMetrics = new ArrayList<JSONMetric>();

        for (JSONMetric metric: jsonMetrics) {
            Set<ConstraintViolation<JSONMetric>> constraintViolations = validator.validate(metric);

            if (constraintViolations.size() == 0) {
                validJsonMetrics.add(metric);
            } else {
                for (ConstraintViolation<JSONMetric> constraintViolation : constraintViolations) {
                    validationErrors.add(new ErrorResponse.ErrorData(tenantId, metric.getMetricName(),
                            constraintViolation.getPropertyPath().toString(), constraintViolation.getMessage()));
                }
            }
        }

        return new JSONMetricsContainer(tenantId, validJsonMetrics, validationErrors);
    }

    @Override
    public void handle(ChannelHandlerContext ctx, FullHttpRequest request) {
        try {

            Tracker.getInstance().track(request);
            requestCount.inc();

            final String tenantId = request.headers().get("tenantId");

            JSONMetricsContainer jsonMetricsContainer;
            List<Metric> validMetrics;

            final Timer.Context jsonTimerContext = jsonTimer.time();

            final String body = request.content().toString(Constants.DEFAULT_CHARSET);
            try {
                jsonMetricsContainer = createContainer(body, tenantId);

                if (jsonMetricsContainer.areDelayedMetricsPresent()) {
                    Tracker.getInstance().trackDelayedMetricsTenant(tenantId, jsonMetricsContainer.getDelayedMetrics());
                }

                validMetrics = jsonMetricsContainer.getValidMetrics();
                forceTTLsIfConfigured(validMetrics);

            } catch (JsonParseException e) {
                log.warn("Exception parsing content", e);
                DefaultHandler.sendResponse(ctx, request, prepareErrorResponse(tenantId, "Cannot parse content"),
                        HttpResponseStatus.BAD_REQUEST);
                return;
            } catch (JsonMappingException e) {
                log.warn("Exception parsing content", e);
                DefaultHandler.sendResponse(ctx, request, prepareErrorResponse(tenantId, "Cannot parse content"),
                        HttpResponseStatus.BAD_REQUEST);
                return;
            } catch (InvalidDataException ex) {
                // todo: we should measure these. if they spike, we track down the bad client.
                // this is strictly a client problem. Something wasn't right (data out of range, etc.)
                log.warn(ctx.channel().remoteAddress() + " " + ex.getMessage());
                DefaultHandler.sendResponse(ctx, request, prepareErrorResponse(tenantId,"Invalid data " + ex.getMessage()),
                        HttpResponseStatus.BAD_REQUEST);
                return;
            } catch (IOException e) {
                log.warn("IO Exception parsing content", e);
                DefaultHandler.sendResponse(ctx, request, prepareErrorResponse(tenantId, "Cannot parse content"),
                        HttpResponseStatus.BAD_REQUEST);
                return;
            } catch (Exception e) {
                log.warn("Other exception while trying to parse content", e);
                DefaultHandler.sendResponse(ctx, request, prepareErrorResponse(tenantId,"Failed parsing content"),
                        HttpResponseStatus.INTERNAL_SERVER_ERROR);
                return;
            } finally {
                jsonTimerContext.stop();
            }

            List<ErrorResponse.ErrorData> validationErrors = jsonMetricsContainer.getValidationErrors();

            // If no valid metrics are present, return error response
            if (validMetrics.isEmpty()) {
                log.warn(ctx.channel().remoteAddress() + " No valid metrics");

                if (validationErrors.isEmpty()) {
                    DefaultHandler.sendResponse(ctx, request, prepareErrorResponse(tenantId, "No valid metrics"),
                            HttpResponseStatus.BAD_REQUEST);
                } else {
                    sendErrorResponse(ctx, request, validationErrors, HttpResponseStatus.BAD_REQUEST);
                }
                return;
            }

            final MetricsCollection collection = new MetricsCollection();
            collection.add(new ArrayList<IMetric>(validMetrics));
            final Timer.Context persistingTimerContext = persistingTimer.time();
            try {
                ListenableFuture<List<Boolean>> futures = processor.apply(collection);
                List<Boolean> persisteds = futures.get(timeout.getValue(), timeout.getUnit());
                for (Boolean persisted : persisteds) {
                    if (!persisted) {
                        log.warn("Trouble persisting metrics:");
                        log.warn(String.format("%s", Arrays.toString(validMetrics.toArray())));
                        DefaultHandler.sendResponse(ctx, request,
                                prepareErrorResponse(tenantId,"Persisted failed for metrics"), HttpResponseStatus.INTERNAL_SERVER_ERROR);
                        return;
                    }
                }

                // after processing metrics, return either OK or MULTI_STATUS depending on number of valid metrics
                if( !validationErrors.isEmpty() ) {
                    // has some validation errors, return MULTI_STATUS
                    sendErrorResponse(ctx, request, validationErrors, HttpResponseStatus.MULTI_STATUS);
                }
                else {
                    // no validation error, return OK
                    DefaultHandler.sendResponse(ctx, request, null, HttpResponseStatus.OK);
                }

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

    private String prepareErrorResponse(String tenantId, String message) {
        return "{\"errors\": [{\"tenantId\": \"" + tenantId + "\", \"message\": \"" + message + "\"}]}";
    }

    private void sendErrorResponse(ChannelHandlerContext ctx, FullHttpRequest request,
                                   List<ErrorResponse.ErrorData> validationErrors, HttpResponseStatus status) {
        final String tenantId = request.headers().get("tenantId");

        try {

            String responseBody = new ObjectMapper().writeValueAsString(new ErrorResponse(validationErrors));
            DefaultHandler.sendResponse(ctx, request, responseBody, status);

        } catch (IOException e) {

            log.warn("Error preparing response", e);
            String responseBody = prepareErrorResponse(tenantId, "Error preparing response");
            DefaultHandler.sendResponse(ctx, request, responseBody, HttpResponseStatus.INTERNAL_SERVER_ERROR);
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
