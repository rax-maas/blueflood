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

import com.google.common.annotations.VisibleForTesting;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.service.HttpConfig;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
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
    private static final String KEEP_ALIVE_TIMEOUT_STR = "timeout=";

    private static final Logger log = LoggerFactory.getLogger(HttpResponder.class);

    private static HttpResponder INSTANCE = new HttpResponder();

    public static HttpResponder getInstance() { return INSTANCE; }

    private int httpConnIdleTimeout =
            Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_CONNECTION_READ_IDLE_TIME_SECONDS);

    @VisibleForTesting
    HttpResponder() {}

    @VisibleForTesting
    HttpResponder(int httpConnIdleTimeout) {
        this.httpConnIdleTimeout = httpConnIdleTimeout;
    }

    public void respond(ChannelHandlerContext ctx, FullHttpRequest req, HttpResponseStatus status) {
        respond(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, status));
    }

    public void respond(ChannelHandlerContext ctx, FullHttpRequest req, FullHttpResponse res) {

        // set response headers
        if (CORS_ENABLED) {
            res.headers().add("Access-Control-Allow-Origin", CORS_ALLOWED_ORIGINS);
        }

        if (res.content() != null) {
            setContentLength(res, res.content().readableBytes());
        }

        boolean isKeepAlive = isKeepAlive(req);
        if (isKeepAlive) {
            res.headers().add(CONNECTION, KEEP_ALIVE);

            // if this config is set to non zero, it means we intend
            // to close this connection after x seconds. We need to
            // respond back with the right headers to indicate this
            if ( httpConnIdleTimeout > 0 ) {
                res.headers().add(KEEP_ALIVE, KEEP_ALIVE_TIMEOUT_STR + httpConnIdleTimeout);
            }
        }

        // Send the response and close the connection if necessary.
        ctx.channel().write(res);
        if (req == null || !isKeepAlive) {
            log.debug("Closing channel. isKeepAlive:" + isKeepAlive + " on channel: " + ctx.channel().toString());
            ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
    }
}