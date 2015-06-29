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

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.*;

import java.util.List;
import java.util.Map;
import java.util.Set;


public class HTTPRequestWithDecodedQueryParams implements HttpRequest {
    private Map<String, List<String>> queryParams;
    private DefaultHttpRequest request;

    private HTTPRequestWithDecodedQueryParams(DefaultHttpRequest request, Map<String, List<String>> queryParams) {
        this.request = request;
        this.queryParams = queryParams;
    }

    public static HTTPRequestWithDecodedQueryParams createHttpRequestWithDecodedQueryParams(DefaultHttpRequest request) {
        final QueryStringDecoder decoder = new QueryStringDecoder(request.getUri());
        request.setUri(decoder.getPath());
        return new HTTPRequestWithDecodedQueryParams(request, decoder.getParameters());
    }

    public Map<String, List<String>> getQueryParams() {
        return queryParams;
    }

    @Override
    public HttpMethod getMethod() {
        return request.getMethod();
    }

    @Override
    public void setMethod(HttpMethod method) {
        request.setMethod(method);
    }

    @Override
    public String getUri() {
        return request.getUri();
    }

    @Override
    public void setUri(String uri) {
        request.setUri(uri);
    }

    @Override
    public String getHeader(String name) {
        return request.getHeader(name);
    }

    @Override
    public List<String> getHeaders(String name) {
        return request.getHeaders(name);
    }

    @Override
    public List<Map.Entry<String, String>> getHeaders() {
        return request.getHeaders();
    }

    @Override
    public boolean containsHeader(String name) {
        return request.containsHeader(name);
    }

    @Override
    public Set<String> getHeaderNames() {
        return request.getHeaderNames();
    }

    @Override
    public HttpVersion getProtocolVersion() {
        return request.getProtocolVersion();
    }

    @Override
    public void setProtocolVersion(HttpVersion version) {
        request.setProtocolVersion(version);
    }

    @Override
    public ChannelBuffer getContent() {
        return request.getContent();
    }

    @Override
    public void setContent(ChannelBuffer content) {
        request.setContent(content);
    }

    @Override
    public void addHeader(String name, Object value) {
        request.addHeader(name, value);
    }

    @Override
    public void setHeader(String name, Object value) {
        request.addHeader(name, value);
    }

    @Override
    public void setHeader(String name, Iterable<?> values) {
        request.setHeader(name, values);
    }

    @Override
    public void removeHeader(String name) {
        request.removeHeader(name);
    }

    @Override
    public void clearHeaders() {
        request.clearHeaders();
    }

    @Deprecated
    public long getContentLength() {
        return request.getContentLength();
    }

    @Deprecated
    public long getContentLength(long defaultValue) {
        return request.getContentLength(defaultValue);
    }

    @Override
    public boolean isChunked() {
        return request.isChunked();
    }

    @Override
    public void setChunked(boolean chunked) {
        request.setChunked(chunked);
    }

    @Deprecated
    public boolean isKeepAlive() {
        return request.isKeepAlive();
    }
}
