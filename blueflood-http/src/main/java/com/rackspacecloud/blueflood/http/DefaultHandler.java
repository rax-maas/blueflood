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
import com.rackspacecloud.blueflood.tracker.Tracker;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.*;

import java.util.Iterator;
import java.util.Map;

public class DefaultHandler implements HttpRequestHandler {
    private static final Timer sendResponseTimer = Metrics.timer(DefaultHandler.class, "HTTP response sending timer");

    @Override
    public void handle(ChannelHandlerContext ctx, HttpRequest request) {
        HttpResponder.respond(ctx, request, HttpResponseStatus.OK);
    }

    public static void sendResponse(ChannelHandlerContext channel, HttpRequest request, String messageBody, HttpResponseStatus status) {
        sendResponse(channel, request, messageBody, null);
    }
    public static void sendResponse(ChannelHandlerContext channel, HttpRequest request, String messageBody, HttpResponseStatus status, Map<String, String> headers) {

        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
        if(headers!=null && !headers.keySet().isEmpty()){
            Iterator<String> itr = headers.keySet().iterator();
            while(itr.hasNext()){
                String headerKey = itr.next();
                response.setHeader(headerKey,headers.get(headerKey));
            }
        }
        final Timer.Context sendResponseTimerContext = sendResponseTimer.time();

        try {
            if (messageBody != null && !messageBody.isEmpty()) {
                response.setContent(ChannelBuffers.copiedBuffer(messageBody, Constants.DEFAULT_CHARSET));
            }

            Tracker.trackResponse(request, response);
            HttpResponder.respond(channel, request, response);
        } finally {
            sendResponseTimerContext.stop();
        }
    }

}