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

import com.rackspacecloud.blueflood.http.HTTPRequestWithDecodedQueryParams;
import com.rackspacecloud.blueflood.io.EventsIO;
import com.rackspacecloud.blueflood.types.Event;
import junit.framework.Assert;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import org.joda.time.DateTime;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

import static org.mockito.Mockito.*;

public class HttpEventsIngestionHandlerTest {

    private EventsIO searchIO;
    private HttpEventsIngestionHandler handler;
    private ChannelHandlerContext context;
    private Channel channel;
    private static final String TENANT = "tenant";

    public HttpEventsIngestionHandlerTest() {
        searchIO = mock(EventsIO.class);
        handler = new HttpEventsIngestionHandler(searchIO);
        channel = mock(Channel.class);
        context = mock(ChannelHandlerContext.class);
        when(context.getChannel()).thenReturn(channel);
        when(channel.write(anyString())).thenReturn(new SucceededChannelFuture(channel));
    }

    private Map<String, Object> createRandomEvent() {
        Event event = new Event() {
            {
                setWhat("1");
                setWhen(2);
                setData("3");
                setTags("4");
            }
        };
        return event.toMap();
    }

    private HttpRequest createPutOneEventRequest(Map<String, Object> event) throws IOException {
        List<Map<String, Object>> events = new ArrayList<Map<String, Object>>();
        events.add(event);
        final String requestBody = new ObjectMapper().writeValueAsString(events.get(0));
        return createRequest(HttpMethod.POST, "", requestBody);
    }

    private HttpRequest createRequest(HttpMethod method, String uri, String requestBody) {
        DefaultHttpRequest rawRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, method, "/v2.0/" + TENANT + "/events/" + uri);
        rawRequest.setHeader("tenantId", TENANT);
        if (!requestBody.equals(""))
            rawRequest.setContent(ChannelBuffers.copiedBuffer(requestBody.getBytes()));
        return HTTPRequestWithDecodedQueryParams.createHttpRequestWithDecodedQueryParams(rawRequest);
    }

    @Test
    public void testElasticSearchInsertCalledWhenPut() throws Exception {
        List<Map<String, Object>> events = new ArrayList<Map<String, Object>>();
        Map<String, Object> event = createRandomEvent();
        events.add(event);
        handler.handle(context, createPutOneEventRequest(event));
        verify(searchIO).insert(TENANT, events);
    }

    @Test public void testMalformedEventPut() throws Exception {
        final String malformedJSON = "{\"when\":, what]}";
        handler.handle(context, createRequest(HttpMethod.POST, "", malformedJSON));
        ArgumentCaptor<DefaultHttpResponse> argument = ArgumentCaptor.forClass(DefaultHttpResponse.class);
        verify(searchIO, never()).insert(anyString(), anyList());
        verify(channel).write(argument.capture());
        Assert.assertNotSame(argument.getValue().getContent().toString(Charset.defaultCharset()), "");
    }

    @Test public void testMinimumEventPut() throws Exception {
        Map<String, Object> event = new HashMap<String, Object>();
        event.put(Event.FieldLabels.data.name(), "data");
        ArgumentCaptor<DefaultHttpResponse> argument = ArgumentCaptor.forClass(DefaultHttpResponse.class);
        handler.handle(context, createPutOneEventRequest(event));
        verify(searchIO, never()).insert(anyString(), anyList());
        verify(channel).write(argument.capture());
        Assert.assertEquals(argument.getValue().getContent().toString(Charset.defaultCharset()), "Invalid Data: Event should contain at least 'what' field.");
    }

    @Test public void testApplyingCurrentTimeWhenEmpty() throws Exception {
        Map<String, Object> event = createRandomEvent();
        event.remove(Event.FieldLabels.when.name());
        handler.handle(context, createPutOneEventRequest(event));
        event.put(Event.FieldLabels.when.name(), convertDateTimeToTimestamp(new DateTime()));
        verify(searchIO).insert(TENANT, Arrays.asList(event));
    }

    private long convertDateTimeToTimestamp(DateTime date) {
        return date.getMillis() / 1000;
    }
}
