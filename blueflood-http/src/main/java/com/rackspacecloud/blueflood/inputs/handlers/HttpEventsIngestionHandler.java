/*
 * Copyright 2015 Rackspace
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
import com.rackspacecloud.blueflood.exceptions.InvalidDataException;
import com.rackspacecloud.blueflood.http.DefaultHandler;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.io.EventsIO;
import com.rackspacecloud.blueflood.outputs.formats.ErrorResponse;
import com.rackspacecloud.blueflood.types.Event;
import com.rackspacecloud.blueflood.utils.Metrics;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.util.*;

public class HttpEventsIngestionHandler implements HttpRequestHandler {

    private static final Logger log = LoggerFactory.getLogger(HttpEventsIngestionHandler.class);

    private EventsIO searchIO;
    private final com.codahale.metrics.Timer httpEventsIngestTimer = Metrics.timer(HttpEventsIngestionHandler.class,
            "Handle HTTP request for ingesting events");

    private static final ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    protected static final Validator validator = factory.getValidator();

    public HttpEventsIngestionHandler(EventsIO searchIO) {
        this.searchIO = searchIO;
    }

    @Override
    public void handle(ChannelHandlerContext ctx, FullHttpRequest request) {

        final String tenantId = request.headers().get(Event.FieldLabels.tenantId.name());
        String response = "";
        ObjectMapper objectMapper = new ObjectMapper();
        final Timer.Context httpEventsIngestTimerContext = httpEventsIngestTimer.time();
        try {
            String body = request.content().toString(0,
                    request.content().writerIndex(),
                    CharsetUtil.UTF_8);
            Event event = objectMapper.readValue(body, Event.class);

            // To verify if the request has multiple elements. If some one sends request with multiple root elements,
            // parser does not throw any validation exceptions. We are checking it manually here.
            Iterator<Event> iterator = objectMapper.reader(Event.class).readValues(body);
            if (iterator.hasNext()) {
                iterator.next();
                if (iterator.hasNext()) { //has more than one element
                    throw new InvalidDataException("Only one event is allowed per request");
                }
            }

            Set<ConstraintViolation<Event>> constraintViolations = validator.validate(event);

            List<ErrorResponse.ErrorData> validationErrors = new ArrayList<ErrorResponse.ErrorData>();
            for (ConstraintViolation<Event> constraintViolation : constraintViolations) {
                validationErrors.add(
                        new ErrorResponse.ErrorData(tenantId, "",
                        constraintViolation.getPropertyPath().toString(), constraintViolation.getMessage(),
                        event.getWhen()));
            }

            if (!validationErrors.isEmpty()) {
                DefaultHandler.sendErrorResponse(ctx, request, validationErrors, HttpResponseStatus.BAD_REQUEST);
                return;
            }

            searchIO.insert(tenantId, Arrays.asList(event.toMap()));
            DefaultHandler.sendResponse(ctx, request, response, HttpResponseStatus.OK);
        } catch (JsonMappingException e) {
            log.debug(String.format("Exception %s", e.toString()));
            response = String.format("Invalid Data: %s", e.getMessage());
            DefaultHandler.sendErrorResponse(ctx, request, response, HttpResponseStatus.BAD_REQUEST);
        } catch (JsonParseException e){
            log.debug(String.format("Exception %s", e.toString()));
            response = String.format("Invalid Data: %s", e.getMessage());
            DefaultHandler.sendErrorResponse(ctx, request, response, HttpResponseStatus.BAD_REQUEST);
        } catch (InvalidDataException e) {
            log.debug(String.format("Exception %s", e.toString()));
            response = String.format("Invalid Data: %s", e.getMessage());
            DefaultHandler.sendErrorResponse(ctx, request, response, HttpResponseStatus.BAD_REQUEST);
        } catch (Exception e) {
            log.error(String.format("Exception %s", e.toString()));
            response = String.format("Error: %s", e.getMessage());
            DefaultHandler.sendErrorResponse(ctx, request, response, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } finally {
            httpEventsIngestTimerContext.stop();
        }
    }
}