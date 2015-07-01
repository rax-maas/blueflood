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

import com.rackspacecloud.blueflood.http.HTTPRequestWithDecodedQueryParams;
import com.rackspacecloud.blueflood.io.EventsIO;
import com.rackspacecloud.blueflood.types.Event;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.*;

import static org.mockito.Mockito.*;

public class HttpEventsQueryHandlerTest {

    private EventsIO searchIO;
    private HttpEventsQueryHandler handler;
    private ChannelHandlerContext context;
    private Channel channel;
    private static final String TENANT = "tenant";

    public HttpEventsQueryHandlerTest() {
        searchIO = mock(EventsIO.class);
        handler = new HttpEventsQueryHandler(searchIO);
        channel = mock(Channel.class);
        context = mock(ChannelHandlerContext.class);
        when(context.getChannel()).thenReturn(channel);
        when(channel.write(anyString())).thenReturn(new SucceededChannelFuture(channel));
    }

    private HttpRequest createGetRequest(String uri) {
        return createRequest(HttpMethod.GET, uri, "");
    }

    private HttpRequest createRequest(HttpMethod method, String uri, String requestBody) {
        DefaultHttpRequest rawRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, method, "/v2.0/" + TENANT + "/events/" + uri);
        rawRequest.setHeader("tenantId", TENANT);
        if (!requestBody.equals(""))
            rawRequest.setContent(ChannelBuffers.copiedBuffer(requestBody.getBytes()));
        return HTTPRequestWithDecodedQueryParams.createHttpRequestWithDecodedQueryParams(rawRequest);
    }

    @Test
    public void testElasticSearchSearchNotCalledEmptyQuery() throws Exception {
        handler.handle(context, createGetRequest(""));
        verify(searchIO, never()).search(TENANT, new HashMap<String, List<String>>());
    }

    private void testQuery(String query, Map<String, List<String>> params) throws Exception {
        handler.handle(context, createGetRequest(query));
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
        return Long.toString(convertDateTimeToTimestamp(new DateTime().withSecondOfMinute(0).withMillisOfSecond(0)));
    }
}
