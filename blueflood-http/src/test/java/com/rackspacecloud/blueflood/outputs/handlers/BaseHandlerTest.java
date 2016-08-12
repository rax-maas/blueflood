package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.http.HttpRequestWithDecodedQueryParams;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpVersion;

public class BaseHandlerTest {

    protected static final String TENANT = "tenant";

    protected FullHttpRequest createGetRequest(String uri) {
        return createRequest(HttpMethod.GET, uri, "");
    }

    private FullHttpRequest createRequest(HttpMethod method, String uri, String requestBody) {
        DefaultFullHttpRequest rawRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, uri);
        rawRequest.headers().set("tenantId", TENANT);
        if (!requestBody.equals(""))
            rawRequest.content().writeBytes(Unpooled.copiedBuffer(requestBody.getBytes()));
        return HttpRequestWithDecodedQueryParams.create(rawRequest);
    }
}
