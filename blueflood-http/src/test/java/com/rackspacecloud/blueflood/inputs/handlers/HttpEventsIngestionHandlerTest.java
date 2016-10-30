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

package com.rackspacecloud.blueflood.inputs.handlers;

import com.rackspacecloud.blueflood.http.HttpRequestWithDecodedQueryParams;
import com.rackspacecloud.blueflood.io.EventsIO;
import com.rackspacecloud.blueflood.outputs.formats.ErrorResponse;
import com.rackspacecloud.blueflood.outputs.handlers.HandlerTestsBase;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.Event;
import com.rackspacecloud.blueflood.utils.DefaultClockImpl;
import io.netty.buffer.Unpooled;
import org.codehaus.jackson.map.ObjectMapper;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

import static org.mockito.Mockito.*;
import static junit.framework.Assert.*;

public class HttpEventsIngestionHandlerTest extends HandlerTestsBase {

    private EventsIO searchIO;
    private HttpEventsIngestionHandler handler;
    private ChannelHandlerContext context;
    private Channel channel;
    private ChannelFuture channelFuture;
    private static final String TENANT = "tenant";

    public HttpEventsIngestionHandlerTest() {
        searchIO = mock(EventsIO.class);
        handler = new HttpEventsIngestionHandler(searchIO);
        channel = mock(Channel.class);
        context = mock(ChannelHandlerContext.class);
        channelFuture = mock(ChannelFuture.class);
        when(context.channel()).thenReturn(channel);
        when(channel.write(anyString())).thenReturn(channelFuture);
    }

    private Map<String, Object> createRandomEvent() {
        Event event = new Event() {
            {
                setWhat("1");
                setWhen( System.currentTimeMillis() );
                setData("3");
                setTags("4");
            }
        };
        return event.toMap();
    }

    private FullHttpRequest createPutOneEventRequest(Map<String, Object> event) throws IOException {
        List<Map<String, Object>> events = new ArrayList<Map<String, Object>>();
        events.add(event);
        final String requestBody = new ObjectMapper().writeValueAsString(events.get(0));
        return createRequest(HttpMethod.POST, "", requestBody);
    }

    private FullHttpRequest createRequest(HttpMethod method, String uri, String requestBody) {
        DefaultFullHttpRequest rawRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, "/v2.0/" + TENANT + "/events/" + uri);
        rawRequest.headers().set("tenantId", TENANT);
        if (!requestBody.equals(""))
            rawRequest.content().writeBytes(Unpooled.copiedBuffer(requestBody.getBytes()));
        return HttpRequestWithDecodedQueryParams.create(rawRequest);
    }

    @Test
    public void testElasticSearchInsertCalledWhenPut() throws Exception {
        List<Map<String, Object>> events = new ArrayList<Map<String, Object>>();
        Map<String, Object> event = createRandomEvent();
        events.add(event);
        handler.handle(context, createPutOneEventRequest(event));
        verify(searchIO).insert(TENANT, events);
    }

    @Test
    public void testInvalidRequestBody() throws Exception {
        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, createRequest(HttpMethod.POST, "", "{\"xxx\": \"yyy\"}"));
        verify(searchIO, never()).insert(anyString(), anyList());
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testMalformedEventPut() throws Exception {
        final String malformedJSON = "{\"when\":, what]}"; //causes JsonParseException
        handler.handle(context, createRequest(HttpMethod.POST, "", malformedJSON));
        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        verify(searchIO, never()).insert(anyString(), anyList());
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testEmptyPut() throws Exception {
        Map<String, Object> event = new HashMap<String, Object>();
        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, createPutOneEventRequest(event));
        verify(searchIO, never()).insert(anyString(), anyList());
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        System.out.println(errorResponse);
        assertEquals("Number of errors invalid", 2, errorResponse.getErrors().size());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testEmptyWhatField() throws Exception {
        Map<String, Object> event = new HashMap<String, Object>();
        event.put(Event.FieldLabels.what.name(), "");
        event.put(Event.FieldLabels.when.name(), System.currentTimeMillis());
        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, createPutOneEventRequest(event));
        verify(searchIO, never()).insert(anyString(), anyList());
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid error message", "may not be empty", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testWhenFieldInThePast() throws Exception {

        long collectionTimeInPast = new DefaultClockImpl().now().getMillis() - 10000
                - Configuration.getInstance().getLongProperty( CoreConfig.BEFORE_CURRENT_COLLECTIONTIME_MS );

        Map<String, Object> event = new HashMap<String, Object>();
        event.put(Event.FieldLabels.what.name(), "xxxx");
        event.put(Event.FieldLabels.when.name(), collectionTimeInPast);
        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, createPutOneEventRequest(event));
        verify(searchIO, never()).insert(anyString(), anyList());
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid error message", "Out of bounds. Cannot be more than 259200000 milliseconds into the past." +
                " Cannot be more than 600000 milliseconds into the future", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testWhenFieldInTheFuture() throws Exception {

        long collectionTimeInFuture = new DefaultClockImpl().now().getMillis() + 10000
                + Configuration.getInstance().getLongProperty( CoreConfig.AFTER_CURRENT_COLLECTIONTIME_MS );

        Map<String, Object> event = new HashMap<String, Object>();
        event.put(Event.FieldLabels.what.name(), "xxxx");
        event.put(Event.FieldLabels.when.name(), collectionTimeInFuture);
        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, createPutOneEventRequest(event));
        verify(searchIO, never()).insert(anyString(), anyList());
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid error message", "Out of bounds. Cannot be more than 259200000 milliseconds into the past." +
                " Cannot be more than 600000 milliseconds into the future", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testMinimumEventPut() throws Exception {
        Map<String, Object> event = new HashMap<String, Object>();
        event.put(Event.FieldLabels.data.name(), "data");
        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, createPutOneEventRequest(event));
        verify(searchIO, never()).insert(anyString(), anyList());
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 2, errorResponse.getErrors().size());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }
}
