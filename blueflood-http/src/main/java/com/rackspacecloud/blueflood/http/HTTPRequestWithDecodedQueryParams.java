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


public class HTTPRequestWithDecodedQueryParams implements FullHttpRequest {
    private Map<String, List<String>> queryParams;
    private DefaultFullHttpRequest request;

    private HTTPRequestWithDecodedQueryParams(DefaultFullHttpRequest request, Map<String, List<String>> queryParams) {
        this.request = request;
        this.queryParams = queryParams;
    }

    public static HTTPRequestWithDecodedQueryParams createHttpRequestWithDecodedQueryParams(DefaultFullHttpRequest request) {
        final QueryStringDecoder decoder = new QueryStringDecoder(request.getUri());
        request.setUri(decoder.path());
        return new HTTPRequestWithDecodedQueryParams(request, decoder.parameters());
    }

    public Map<String, List<String>> getQueryParams() {
        return queryParams;
    }

    @Override
    public HttpMethod getMethod() {
        return request.getMethod();
    }

    @Override
    public FullHttpRequest setMethod(HttpMethod httpMethod) {
        return request.setMethod(httpMethod);
    }

    @Override
    public String getUri() {
        return request.getUri();
    }

    @Override
    public FullHttpRequest setUri(String uri) {
        return request.setUri(uri);
    }

    @Override
    public HttpVersion getProtocolVersion() {
        return request.getProtocolVersion();
    }

    @Override
    public HttpHeaders trailingHeaders() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ByteBuf content() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public FullHttpRequest copy() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public HttpContent duplicate() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public FullHttpRequest retain(int i) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean release() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean release(int i) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int refCnt() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public FullHttpRequest retain() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public FullHttpRequest setProtocolVersion(HttpVersion httpVersion) {
        return request.setProtocolVersion(httpVersion);
    }

    @Override
    public HttpHeaders headers() {
        return request.headers();
    }

    @Override
    public DecoderResult getDecoderResult() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setDecoderResult(DecoderResult decoderResult) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
