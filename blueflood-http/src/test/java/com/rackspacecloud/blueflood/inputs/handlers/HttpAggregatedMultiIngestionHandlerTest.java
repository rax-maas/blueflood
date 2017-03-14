/*
 * Copyright 2014 Rackspace
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

import com.codahale.metrics.Meter;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.rackspacecloud.blueflood.inputs.formats.AggregatedPayload;
import com.rackspacecloud.blueflood.io.Instrumentation;
import com.rackspacecloud.blueflood.io.serializers.Serializers;
import com.rackspacecloud.blueflood.outputs.formats.ErrorResponse;
import com.rackspacecloud.blueflood.outputs.handlers.HandlerTestsBase;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.DefaultClockImpl;
import com.rackspacecloud.blueflood.utils.TimeValue;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.rackspacecloud.blueflood.TestUtils.*;
import static junit.framework.Assert.assertEquals;
import static org.mockito.Matchers.*;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;


public class HttpAggregatedMultiIngestionHandlerTest extends HandlerTestsBase {

    private HttpAggregatedMultiIngestionHandler handler;
    private HttpMetricsIngestionServer.Processor processor;

    private ChannelHandlerContext context;
    private Channel channel;
    private ChannelFuture channelFuture;

    private Meter ingestedMetrics;
    private Meter ingestedDelayedMetrics;

    private static final String TENANT = "tenant";

    private List<AggregatedPayload> bundleList;

    private final String postfix = ".pref";

    @Before
    public void buildBundle() throws Exception {
        processor = mock(HttpMetricsIngestionServer.Processor.class);
        handler = new HttpAggregatedMultiIngestionHandler(processor, new TimeValue(5, TimeUnit.SECONDS));

        channel = mock(Channel.class);
        context = mock(ChannelHandlerContext.class);
        channelFuture = mock(ChannelFuture.class);
        when(context.channel()).thenReturn(channel);
        when(channel.write(anyString())).thenReturn(channelFuture);
        ListenableFuture mockFuture = mock(ListenableFuture.class);
        when(processor.apply(any(MetricsCollection.class))).thenReturn(mockFuture);
        when(mockFuture.get(anyLong(), any(TimeUnit.class))).thenReturn(new ArrayList<Boolean>());

        String json = getJsonFromFile("sample_multi_aggregated_payload.json", postfix);
        bundleList = HttpAggregatedMultiIngestionHandler.createBundleList(json);

        ingestedMetrics = Instrumentation.getIngestedMetricsMeter(TENANT);
        ingestedDelayedMetrics = Instrumentation.getIngestedDelayedMetricsMeter(TENANT);
    }

    @Test
    public void testMultiBundle() {
        HashSet<String> tenantIdSet = new HashSet<String>();
        HashSet<Long> timestampSet = new HashSet<Long>();
        Assert.assertTrue(bundleList.size() == 3);

        for (AggregatedPayload bundle : bundleList) {
            tenantIdSet.add(bundle.getTenantId());
            timestampSet.add(bundle.getTimestamp());
        }

        Assert.assertTrue(tenantIdSet.size() == 3); //3 unique timestamps are supported
        Assert.assertTrue(timestampSet.size() == 3); //3 unique tenants are supported
    }

    @Test
    public void testCounters() {
        for (AggregatedPayload bundle : bundleList) {
            Collection<PreaggregatedMetric> counters = PreaggregateConversions.convertCounters("1", 1, 15000, bundle.getCounters());
            Assert.assertEquals(1, counters.size());
            ensureSerializability(counters);
        }
    }

    @Test
    public void testEmptyButValidMultiJSON() {
        String badJson = "[]";
        List<AggregatedPayload> bundle = HttpAggregatedMultiIngestionHandler.createBundleList(badJson);
    }
    
    @Test
    public void testGauges() {
        for (AggregatedPayload bundle : bundleList) {
            Collection<PreaggregatedMetric> gauges = PreaggregateConversions.convertGauges("1", 1, bundle.getGauges());
            Assert.assertEquals(1, gauges.size());
            ensureSerializability(gauges);
        }
    }
     
    @Test
    public void testSets() {
        for (AggregatedPayload bundle : bundleList) {
            Collection<PreaggregatedMetric> sets = PreaggregateConversions.convertSets("1", 1, bundle.getSets());
            Assert.assertEquals(1, sets.size());
            ensureSerializability(sets);
        }
    }
    
    @Test
    public void testTimers() {
        for (AggregatedPayload bundle : bundleList) {
            Collection<PreaggregatedMetric> timers = PreaggregateConversions.convertTimers("1", 1, bundle.getTimers());
            Assert.assertEquals(1, timers.size());
            ensureSerializability(timers);
        }
    }
    
    // ok. while we're out it, let's test serialization. Just for fun. The reasoning is that these metrics
    // follow a different creation path that what we currently have in tests.
    private static void ensureSerializability(Collection<PreaggregatedMetric> metrics) {
        for (PreaggregatedMetric metric : metrics) {
            AbstractSerializer serializer = Serializers.serializerFor(metric.getMetricValue().getClass());
            serializer.toByteBuffer(metric.getMetricValue());
        }
    }

    @Test
    public void testEmptyRequest() throws IOException {
        String requestBody = "";
        FullHttpRequest request = createIngestRequest(requestBody);

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "Invalid request body", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testNonArrayJsonRequest() throws IOException {
        String requestBody = "{}"; //causes JsonMappingException
        FullHttpRequest request = createIngestRequest(requestBody);

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "Invalid request body", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testEmptyArrayJsonRequest() throws IOException {
        String requestBody = "[]"; //causes JsonMappingException
        FullHttpRequest request = createIngestRequest(requestBody);

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String responseBody = argument.getValue().content().toString(Charset.defaultCharset());

        assertEquals("Invalid response", "No valid metrics", responseBody);
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void perTenantMetricsOn_emptyRequest_shouldNotRecordAnything() throws IOException {
        String requestBody = "[]";
        FullHttpRequest request = createIngestRequest(requestBody);

        long ingestedMetricsBefore = ingestedMetrics.getCount();
        long ingestedDelayedMetricsBefore = ingestedDelayedMetrics.getCount();

        HttpAggregatedMultiIngestionHandler handler = spy(new HttpAggregatedMultiIngestionHandler(processor, new TimeValue(5, TimeUnit.SECONDS), true));
        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());
        verify(handler, never()).recordPerTenantMetrics(eq(TENANT), anyInt(), anyInt());

        assertEquals("ingested metrics count", 0, ingestedMetrics.getCount() - ingestedMetricsBefore);
        assertEquals("ingested delayed metrics count", 0, ingestedDelayedMetrics.getCount() - ingestedDelayedMetricsBefore);
    }

    @Test
    public void perTenantMetricsOn_shouldRecordDelayedMetrics() throws Exception {
        long delayedTime = new DefaultClockImpl().now().getMillis() - 100 -
                Configuration.getInstance().getLongProperty(CoreConfig.ROLLUP_DELAY_MILLIS);
        FullHttpRequest request = createIngestRequest(
                getJsonFromFile("sample_multi_aggregated_payload.json", delayedTime, postfix));

        long ingestedMetricsBefore = ingestedMetrics.getCount();
        long ingestedDelayedMetricsBefore = ingestedDelayedMetrics.getCount();

        ListenableFuture<List<Boolean>> futures = mock(ListenableFuture.class);
        List<Boolean> answers = new ArrayList<>();
        answers.add(Boolean.TRUE);
        when(processor.apply(any())).thenReturn(futures);
        when(futures.get(anyLong(), any())).thenReturn(answers);

        HttpAggregatedMultiIngestionHandler handler = spy(new HttpAggregatedMultiIngestionHandler(processor, new TimeValue(5, TimeUnit.SECONDS), true));
        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());
        verify(handler, times(1)).recordPerTenantMetrics(eq(TENANT), eq(0), eq(12));

        assertEquals("ingested metrics count", 0, ingestedMetrics.getCount() - ingestedMetricsBefore);
        assertEquals("ingested delayed metrics count", 12, ingestedDelayedMetrics.getCount() - ingestedDelayedMetricsBefore);
    }

    @Test
    public void perTenantMetricsOn_shouldRecordNonDelayedMetrics() throws Exception {
        long timestamp = new DefaultClockImpl().now().getMillis();
        FullHttpRequest request = createIngestRequest(
                getJsonFromFile("sample_multi_aggregated_payload.json", timestamp, postfix));

        long ingestedMetricsBefore = ingestedMetrics.getCount();
        long ingestedDelayedMetricsBefore = ingestedDelayedMetrics.getCount();

        ListenableFuture<List<Boolean>> futures = mock(ListenableFuture.class);
        List<Boolean> answers = new ArrayList<>();
        answers.add(Boolean.TRUE);
        when(processor.apply(any())).thenReturn(futures);
        when(futures.get(anyLong(), any())).thenReturn(answers);

        HttpAggregatedMultiIngestionHandler handler = spy(new HttpAggregatedMultiIngestionHandler(processor, new TimeValue(5, TimeUnit.SECONDS), true));
        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);

        verify(channel).write(argument.capture());
        verify(handler, times(1)).recordPerTenantMetrics(eq(TENANT), eq(12), eq(0));

        assertEquals("ingested metrics count", 12, ingestedMetrics.getCount() - ingestedMetricsBefore);
        assertEquals("ingested delayed metrics count", 0, ingestedDelayedMetrics.getCount() - ingestedDelayedMetricsBefore);
    }

    @Test
    public void perTenantMetricsOff_shouldNotRecordMetrics() throws Exception {
        long timestamp = new DefaultClockImpl().now().getMillis();
        FullHttpRequest request = createIngestRequest(
                getJsonFromFile("sample_multi_aggregated_payload.json", timestamp, postfix));

        long ingestedMetricsBefore = ingestedMetrics.getCount();
        long ingestedDelayedMetricsBefore = ingestedDelayedMetrics.getCount();

        ListenableFuture<List<Boolean>> futures = mock(ListenableFuture.class);
        List<Boolean> answers = new ArrayList<>();
        answers.add(Boolean.TRUE);
        when(processor.apply(any())).thenReturn(futures);
        when(futures.get(anyLong(), any())).thenReturn(answers);

        // turn off per tenant metrics tracking
        HttpAggregatedMultiIngestionHandler handler = spy(new HttpAggregatedMultiIngestionHandler(processor, new TimeValue(5, TimeUnit.SECONDS), false));
        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());
        verify(handler, times(1)).recordPerTenantMetrics(eq(TENANT), eq(12), eq(0));

        assertEquals("ingested metrics count", 0, ingestedMetrics.getCount() - ingestedMetricsBefore);
        assertEquals("ingested delayed metrics count", 0, ingestedDelayedMetrics.getCount() - ingestedDelayedMetricsBefore);

    }

    private FullHttpRequest createIngestRequest(String requestBody) {
        return super.createPostRequest("/v2.0/" + TENANT + "/aggregated/multi", requestBody);
    }
}
