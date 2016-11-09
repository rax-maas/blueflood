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

package com.rackspacecloud.blueflood.http;

import com.codahale.metrics.Timer;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.outputs.formats.ErrorResponse;
import com.rackspacecloud.blueflood.tracker.Tracker;
import com.rackspacecloud.blueflood.utils.Metrics;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import org.apache.commons.lang.StringEscapeUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DefaultHandler implements HttpRequestHandler {
    private static final Timer sendResponseTimer = Metrics.timer(DefaultHandler.class, "HTTP response sending timer");

    private static final Logger log = LoggerFactory.getLogger(DefaultHandler.class);

    @Override
    public void handle(ChannelHandlerContext ctx, FullHttpRequest request) {
        HttpResponder.respond(ctx, request, HttpResponseStatus.OK);
    }

    public static void sendErrorResponse(ChannelHandlerContext ctx, FullHttpRequest request,
                                   List<ErrorResponse.ErrorData> validationErrors, HttpResponseStatus status) {
        try {

            String responseBody = new ObjectMapper().writeValueAsString(new ErrorResponse(validationErrors));
            sendResponse(ctx, request, responseBody, status);

        } catch (IOException e) {

            log.error("Error preparing response", e);
            sendErrorResponse(ctx, request, "Error preparing response", HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }

    public static void sendErrorResponse(ChannelHandlerContext ctx, FullHttpRequest request,
                                   final String message, HttpResponseStatus status) {
        final String tenantId = request.headers().get("tenantId");

        List<ErrorResponse.ErrorData> errrors = new ArrayList<ErrorResponse.ErrorData>(){{
            add(new ErrorResponse.ErrorData(tenantId, null, null, message, null));
        }};

        sendErrorResponse(ctx, request, errrors, status);
    }

    public static void sendResponse(ChannelHandlerContext channel, FullHttpRequest request,
                                    String messageBody, HttpResponseStatus status) {
        sendResponse(channel, request, messageBody, status, null);
    }
    
    public static void sendResponse(ChannelHandlerContext channel, FullHttpRequest request, String messageBody,
                                    HttpResponseStatus status, Map<String, String> headers) {

        DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                                                    status);
        if (headers!=null && !headers.keySet().isEmpty()){
            Iterator<String> itr = headers.keySet().iterator();
            while(itr.hasNext()){
                String headerKey = itr.next();
                response.headers().add(headerKey, headers.get(headerKey));
            }
        }

        final Timer.Context sendResponseTimerContext = sendResponseTimer.time();
        try {
            if (messageBody != null && !messageBody.isEmpty()) {
                response.content().writeBytes(Unpooled.copiedBuffer(messageBody, Constants.DEFAULT_CHARSET));
            }

            HttpResponder.respond(channel, request, response);
            Tracker.getInstance().trackResponse(request, response);
        } finally {
            sendResponseTimerContext.stop();
        }
    }

}