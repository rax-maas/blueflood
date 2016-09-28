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

package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.io.EventsIO;
import com.rackspacecloud.blueflood.outputs.formats.ErrorResponse;
import com.rackspacecloud.blueflood.types.Event;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.nio.charset.Charset;
import java.util.*;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class HttpEventsQueryHandlerTest extends HandlerTestsBase {

    private EventsIO searchIO;
    private HttpEventsQueryHandler handler;
    private ChannelHandlerContext context;
    private Channel channel;
    private ChannelFuture channelFuture;
    private DateTime nowDateTime;

    @Before
    public void setup() {
        nowDateTime = new DateTime().withSecondOfMinute(0).withMillisOfSecond(0);
    }

    public HttpEventsQueryHandlerTest() {
        searchIO = mock(EventsIO.class);
        handler = new HttpEventsQueryHandler(searchIO);
        channel = mock(Channel.class);
        channelFuture = mock(ChannelFuture.class);
        context = mock(ChannelHandlerContext.class);
        when(context.channel()).thenReturn(channel);
        when(channel.write(anyString())).thenReturn(channelFuture);
    }


    @Test
    public void testElasticSearchSearchNotCalledEmptyQuery() throws Exception {
        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, createGetRequest("/v2.0/" + TENANT + "/events/"));
        verify(channel).write(argument.capture());
        verify(searchIO, never()).search(TENANT, new HashMap<String, List<String>>());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "Error: Query should contain at least one query parameter", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    private void testQuery(String query, Map<String, List<String>> params) throws Exception {
        handler.handle(context, createGetRequest("/v2.0/" + TENANT + "/events/" + query));
        verify(searchIO).search(TENANT, params);
    }


    @Test public void testQueryParametersParse() throws Exception {
        Map<String, List<String>> params = new HashMap<String, List<String>>();
        params.put(Event.untilParameterName, Arrays.asList(nowTimestamp()));
        testQuery("?until=now", params);

        params.clear();
        params.put(Event.untilParameterName, Arrays.asList(nowTimestamp()));
        params.put(Event.fromParameterName, Arrays.asList("1422828000"));
        testQuery("?until=now&from=1422828000", params);

        params.clear();
        params.put(Event.tagsParameterName, Arrays.asList("event"));
        testQuery("?tags=event", params);
    }

    @Test
    public void testDateQueryParamProcessing() throws Exception {
        Map<String, List<String>> params = new HashMap<String, List<String>>();

        params.clear();
        params.put(Event.untilParameterName, Arrays.asList(nowTimestamp()));
        params.put(Event.fromParameterName, Arrays.asList(Long.toString(convertDateTimeToTimestamp(new DateTime(2014, 12, 30, 0, 0, 0, 0)))));
        testQuery("?until=now&from=00:00_2014_12_30", params);
    }


    private long convertDateTimeToTimestamp(DateTime date) {
        return date.getMillis() / 1000;
    }

    private String nowTimestamp() {
        return Long.toString(convertDateTimeToTimestamp(nowDateTime));
    }
}
