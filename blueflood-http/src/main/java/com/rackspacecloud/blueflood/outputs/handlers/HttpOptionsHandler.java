package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.http.DefaultHandler;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.tracker.Tracker;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.HashMap;
import java.util.Map;

/**
 * class to handle Cross-Origin Resource Sharing (CORS) OPTIONS requests
 */
public class HttpOptionsHandler implements HttpRequestHandler {

    @Override
    public void handle(ChannelHandlerContext ctx, HttpRequest request) {
        // log the request if tracking enabled
        Tracker.getInstance().track(request);

        // set CORS headers in the response
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Access-Control-Allow-Origin", Configuration.getInstance().getStringProperty(CoreConfig.CORS_ALLOWED_ORIGINS));
        headers.put("Access-Control-Allow-Methods", Configuration.getInstance().getStringProperty(CoreConfig.CORS_ALLOWED_METHODS));
        headers.put("Access-Control-Allow-Headers", Configuration.getInstance().getStringProperty(CoreConfig.CORS_ALLOWED_HEADERS));
        headers.put("Access-Control-Max-Age", Configuration.getInstance().getStringProperty(CoreConfig.CORS_ALLOWED_MAX_AGE));
        DefaultHandler.sendResponse(ctx, request, null, HttpResponseStatus.NO_CONTENT, headers);
    }

}
