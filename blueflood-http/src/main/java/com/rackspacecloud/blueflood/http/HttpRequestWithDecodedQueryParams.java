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

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.*;

import java.util.List;
import java.util.Map;

/**
 * This class is a special type of {@link io.netty.handler.codec.http.HttpRequest}
 * that knows how to decode the query parameters.
 */
public class HttpRequestWithDecodedQueryParams implements FullHttpRequest {

    private Map<String, List<String>> queryParams;
    protected FullHttpRequest request;

    protected HttpRequestWithDecodedQueryParams(FullHttpRequest request, Map<String, List<String>> queryParams) {
        this.request = request;
        this.queryParams = queryParams;
    }

    public static HttpRequestWithDecodedQueryParams create(FullHttpRequest request) {
        final QueryStringDecoder decoder = new QueryStringDecoder(request.getUri());
        request.setUri(decoder.path());
        return new HttpRequestWithDecodedQueryParams(request, decoder.parameters());
    }

    public Map<String, List<String>> getQueryParams() {
        return queryParams;
    }

    // --------------------
    // from FullHttpRequest
    @Override
    public HttpRequestWithDecodedQueryParams copy() {
        return create(request.copy());
    }

    @Override
    public FullHttpRequest retain(int increment) {
        return request.retain(increment);
    }

    @Override
    public FullHttpRequest retain() {
        return request.retain();
    }

    @Override
    public ByteBuf content() {
        return request.content();
    }
    // --------------------
    
    // --------------------
    // from HttpRequest
    @Override
    public String getUri() {
        return request.getUri();
    }

    @Override
    public FullHttpRequest setUri(String uri) {
        return request.setUri(uri);
    }

    @Override
    public FullHttpRequest setProtocolVersion(HttpVersion httpVersion) {
        return request.setProtocolVersion(httpVersion);
    }

    @Override
    public HttpMethod getMethod() {
        return request.getMethod();
    }

    @Override
    public FullHttpRequest setMethod(HttpMethod httpMethod) {
        return request.setMethod(httpMethod);
    }
    // --------------------

    // --------------------
    // from HttpMessage
    @Override
    public HttpVersion getProtocolVersion() {
        return request.getProtocolVersion();
    }
    // --------------------

    // --------------------
    // from HttpObject
    @Override
    public DecoderResult getDecoderResult() {
        return request.getDecoderResult();
    }

    @Override
    public void setDecoderResult(DecoderResult decoderResult) {
        request.setDecoderResult(decoderResult);
    }
    // --------------------

    // --------------------
    // from HttpMessage
    public HttpHeaders headers() {
        return request.headers();
    }
    // --------------------

    // --------------------
    // from LastHttpContent
    @Override
    public HttpHeaders trailingHeaders() {
        return request.trailingHeaders();
    }
    // --------------------

    // --------------------
    // from HttpContent
    @Override
    public HttpContent duplicate() {
        return request.duplicate();
    }
    // --------------------

    // --------------------
    // from ReferencedCounted
    @Override
    public boolean release() {
        return request.release();
    }

    @Override
    public boolean release(int decrement) {
        return request.release(decrement);
    }

    @Override
    public int refCnt() {
        return request.refCnt();
    }
    // --------------------

}
