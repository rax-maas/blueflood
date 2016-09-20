package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.http.HttpRequestWithDecodedQueryParams;
import com.rackspacecloud.blueflood.outputs.formats.ErrorResponse;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class HandlerTestsBase {

    protected static final String TENANT = "tenant";

    protected FullHttpRequest createGetRequest(String uri) {
        return createRequest(HttpMethod.GET, uri, "");
    }

    protected FullHttpRequest createPostRequest(String uri, String requestBody) {
        return createRequest(HttpMethod.POST, uri, requestBody);
    }

    private FullHttpRequest createRequest(HttpMethod method, String uri, String requestBody) {
        DefaultFullHttpRequest rawRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, uri);
        rawRequest.headers().set("tenantId", TENANT);
        if (!requestBody.equals(""))
            rawRequest.content().writeBytes(Unpooled.copiedBuffer(requestBody.getBytes()));
        return HttpRequestWithDecodedQueryParams.create(rawRequest);
    }

    protected ErrorResponse getErrorResponse(String error) throws IOException {
        return new ObjectMapper().readValue(error, ErrorResponse.class);
    }
}
