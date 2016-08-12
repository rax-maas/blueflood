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

import com.rackspacecloud.blueflood.exceptions.InvalidDataException;
import com.rackspacecloud.blueflood.http.DefaultHandler;
import com.codahale.metrics.Timer;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.io.EventsIO;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.Event;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.codehaus.jackson.map.ObjectMapper;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class HttpEventsIngestionHandler implements HttpRequestHandler {

    private static final long pastDiff = Configuration.getInstance().getLongProperty( CoreConfig.BEFORE_CURRENT_COLLECTIONTIME_MS );
    private static final long futureDiff = Configuration.getInstance().getLongProperty( CoreConfig.AFTER_CURRENT_COLLECTIONTIME_MS );

    private static final Logger log = LoggerFactory.getLogger(HttpEventsIngestionHandler.class);
    private EventsIO searchIO;
    private final com.codahale.metrics.Timer httpEventsIngestTimer = Metrics.timer(HttpEventsIngestionHandler.class,
            "Handle HTTP request for ingesting events");

    public HttpEventsIngestionHandler(EventsIO searchIO) {
        this.searchIO = searchIO;
    }

    @Override
    public void handle(ChannelHandlerContext ctx, FullHttpRequest request) {
        final String tenantId = request.headers().get(Event.FieldLabels.tenantId.name());
        HttpResponseStatus status = HttpResponseStatus.OK;
        String response = "";
        ObjectMapper objectMapper = new ObjectMapper();
        final Timer.Context httpEventsIngestTimerContext = httpEventsIngestTimer.time();
        try {
            String body = request.content().toString(0,
                    request.content().writerIndex(),
                    CharsetUtil.UTF_8);
            Event event = objectMapper.readValue(body, Event.class);

            long timestamp = event.getWhen();
            long current = System.currentTimeMillis();

            if (event.getWhat().equals("")) {
                throw new InvalidDataException(String.format( HttpMetricsIngestionHandler.ERROR_HEADER + System.lineSeparator() + "Event should contain at least '%s' field.", Event.FieldLabels.what.name()));
            }

            if ( timestamp > current + futureDiff ) {
                throw new InvalidDataException( HttpMetricsIngestionHandler.ERROR_HEADER + System.lineSeparator() + "'" + event.getWhat() + "': 'when' '" + timestamp + "' is more than '" + futureDiff + "' milliseconds into the future." );
            }

            if ( timestamp < current - pastDiff ) {
                throw new InvalidDataException( HttpMetricsIngestionHandler.ERROR_HEADER + System.lineSeparator() + "'" + event.getWhat() + "': 'when' '" + timestamp + "' is more than '" + pastDiff + "' milliseconds into the past." );
            }

            searchIO.insert(tenantId, Arrays.asList(event.toMap()));
        } catch (JsonMappingException e) {
            log.error(String.format("Exception %s", e.toString()));
            response = String.format("Invalid Data: %s", e.getMessage());
            status = HttpResponseStatus.BAD_REQUEST;
        } catch (JsonParseException e){
            log.error(String.format("Exception %s", e.toString()));
            response = String.format("Invalid Data: %s", e.getMessage());
            status = HttpResponseStatus.BAD_REQUEST;
        } catch (InvalidDataException e) {
            log.error(String.format("InvalidDataException %s", e.toString()));
            response = String.format("Invalid Data: %s", e.getMessage());
            status = HttpResponseStatus.BAD_REQUEST;
        } catch (Exception e) {
            log.error(String.format("Exception %s", e.toString()));
            response = String.format("Error: %s", e.getMessage());
            status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
        } finally {
            DefaultHandler.sendResponse(ctx, request, response, status);
            httpEventsIngestTimerContext.stop();
        }
    }
}