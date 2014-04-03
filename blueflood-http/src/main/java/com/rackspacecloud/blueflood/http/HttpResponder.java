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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;

import static io.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class HttpResponder {
    private static final DefaultFullHttpResponse defaultResponse = new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.OK);

    public static void respond(ChannelHandlerContext ctx, FullHttpRequest req, HttpResponseStatus status) {
        defaultResponse.setStatus(status);
        respond(ctx, req, defaultResponse);
    }

    public static void respond(ChannelHandlerContext ctx, FullHttpRequest req, FullHttpResponse res) {
        if (res.content() != null) {
            HttpHeaders.setContentLength(res, res.content().readableBytes());
        }
        // Send the response and close the connection if necessary.
        ChannelFuture f = ctx.writeAndFlush(res);
        if (!isKeepAlive(req)) {
            f.addListener(ChannelFutureListener.CLOSE);
        }
    }
}