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

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.*;

import java.util.Set;

public class UnsupportedMethodHandler implements HttpRequestHandler {
    private final RouteMatcher routeMatcher;
    private final HttpResponse response;

    public UnsupportedMethodHandler(RouteMatcher router) {
        this.routeMatcher = router;
        this.response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.METHOD_NOT_ALLOWED);
    }

    @Override
    public void handle(ChannelHandlerContext context, HttpRequest request) {
        final Set<String> supportedMethods = routeMatcher.getSupportedMethodsForURL(request.getUri());

        StringBuilder result = new StringBuilder();
        for(String string : supportedMethods) {
            result.append(string);
            result.append(",");
        }
        final String methodsAllowed =  result.length() > 0 ? result.substring(0, result.length() - 1): "";
        response.setHeader("Allow", methodsAllowed);
        HttpResponder.respond(context, request, response);
    }
}