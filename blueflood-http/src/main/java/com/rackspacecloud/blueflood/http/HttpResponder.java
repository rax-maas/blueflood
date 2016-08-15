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

import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpHeaders.Values.*;
import static io.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static io.netty.handler.codec.http.HttpHeaders.setContentLength;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class HttpResponder {

    private static final boolean CORS_ENABLED = Configuration.getInstance().getBooleanProperty(CoreConfig.CORS_ENABLED);
    private static final String CORS_ALLOWED_ORIGINS = Configuration.getInstance().getStringProperty(CoreConfig.CORS_ALLOWED_ORIGINS);

    public static void respond(ChannelHandlerContext ctx, FullHttpRequest req, HttpResponseStatus status) {
        respond(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, status));
    }

    public static void respond(ChannelHandlerContext ctx, FullHttpRequest req, FullHttpResponse res) {

        // set response headers
        if (CORS_ENABLED) {
            res.headers().add("Access-Control-Allow-Origin", CORS_ALLOWED_ORIGINS);
        }


        if (res.content() != null) {
            setContentLength(res, res.content().readableBytes());
        }

        if ( isKeepAlive(req) ) {
            res.headers().add(CONNECTION, KEEP_ALIVE);
        }

        // Send the response and close the connection if necessary.
        ctx.channel().write(res);
        if (req == null || !isKeepAlive(req)) {
            ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
    }
}