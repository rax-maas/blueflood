package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.http.DefaultHandler;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.tracker.Tracker;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.HashMap;
import java.util.Map;

/**
 * class to handle Cross-Origin Resource Sharing (CORS) OPTIONS requests
 */
public class HttpOptionsHandler implements HttpRequestHandler {

    private final boolean CORS_ENABLED = Configuration.getInstance().getBooleanProperty(CoreConfig.CORS_ENABLED);
    private final String CORS_ALLOWED_ORIGINS = Configuration.getInstance().getStringProperty(CoreConfig.CORS_ALLOWED_ORIGINS);
    private final String CORS_ALLOWED_METHODS = Configuration.getInstance().getStringProperty(CoreConfig.CORS_ALLOWED_METHODS);
    private final String CORS_ALLOWED_HEADERS = Configuration.getInstance().getStringProperty(CoreConfig.CORS_ALLOWED_HEADERS);
    private final String CORS_ALLOWED_MAX_AGE = Configuration.getInstance().getStringProperty(CoreConfig.CORS_ALLOWED_MAX_AGE);

    @Override
    public void handle(ChannelHandlerContext ctx, FullHttpRequest request) {
        // log the request if tracking enabled
        Tracker.getInstance().track(request);

        // set CORS headers in the response
        Map<String, String> headers = new HashMap<String, String>();
        if (CORS_ENABLED) {
            headers.put("Access-Control-Allow-Origin", CORS_ALLOWED_ORIGINS);
            headers.put("Access-Control-Allow-Methods", CORS_ALLOWED_METHODS);
            headers.put("Access-Control-Allow-Headers", CORS_ALLOWED_HEADERS);
            headers.put("Access-Control-Max-Age", CORS_ALLOWED_MAX_AGE);
        }
        DefaultHandler.sendResponse(ctx, request, null, HttpResponseStatus.NO_CONTENT, headers);
    }

}
