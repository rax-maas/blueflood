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

import com.google.gson.Gson;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.http.HttpResponder;
import com.rackspacecloud.blueflood.inputs.handlers.wrappers.Bundle;
import com.rackspacecloud.blueflood.io.Constants;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class HttpStatsDIngestionHandler implements HttpRequestHandler {
    private static final Logger logger = LoggerFactory.getLogger(HttpStatsDIngestionHandler.class);
    
    public HttpStatsDIngestionHandler() {
    }
    
    // our own stuff.
    @Override
    public void handle(ChannelHandlerContext ctx, HttpRequest request) {
        // ok. let's see whats in the headers, etc. I need to find out what's been put in, etc.
        logger.info("HEADERS...");
        for (Map.Entry header : request.getHeaders()) {
            logger.info(String.format("%s = %s", header.getKey().toString(), header.getValue().toString()));
        }
        
        // this is all JSON.
        final String body = request.getContent().toString(Constants.DEFAULT_CHARSET);

        Bundle bundle = new Gson().fromJson(body, Bundle.class);
        logger.info(String.format("BUNDLE: %s", bundle.toString()));
        
        
        HttpResponder.respond(ctx, request, HttpResponseStatus.OK);
    }

    
}
