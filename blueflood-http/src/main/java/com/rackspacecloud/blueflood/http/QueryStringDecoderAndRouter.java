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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryStringDecoderAndRouter extends ChannelInboundHandlerAdapter {
    private static final Logger log = LoggerFactory.getLogger(QueryStringDecoderAndRouter.class);
    private final RouteMatcher router;

    public QueryStringDecoderAndRouter(RouteMatcher router) {
        this.router = router;
    }

    @Override
    public void channelRead(ChannelHandlerContext channelHandlerContext, Object msg) throws Exception {
        if (msg instanceof HttpRequest) {
            DefaultFullHttpRequest request = (DefaultFullHttpRequest) msg;
            router.route(channelHandlerContext, request);
        } else {
            log.error("Ignoring non HTTP message {}, from {}", msg, channelHandlerContext.channel().remoteAddress());
            throw new Exception("Non-HTTP message " + msg + " from " + channelHandlerContext.channel().remoteAddress());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("Exception event received: ", cause.getCause());
        ctx.close();
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }
}
