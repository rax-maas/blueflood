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