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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.internal.LazilyParsedNumber;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.rackspacecloud.blueflood.inputs.formats.AggregatedPayload;
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
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.rackspacecloud.blueflood.TestUtils.getJsonFromFile;
import static junit.framework.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;


public class HttpAggregatedIngestionHandlerTest extends HandlerTestsBase {

    private String payloadJson;
    private HttpAggregatedIngestionHandler handler;
    private HttpMetricsIngestionServer.Processor processor;

    private ChannelHandlerContext context;
    private Channel channel;
    private ChannelFuture channelFuture;


    private final String postfix = ".post";
    private static final String TENANT = "tenant";


    @Before
    public void setup() throws Exception {
        processor = mock(HttpMetricsIngestionServer.Processor.class);
        handler = new HttpAggregatedIngestionHandler(processor, new TimeValue(5, TimeUnit.SECONDS));

        channel = mock(Channel.class);
        context = mock(ChannelHandlerContext.class);
        channelFuture = mock(ChannelFuture.class);
        when(context.channel()).thenReturn(channel);
        when(channel.write(anyString())).thenReturn(channelFuture);
        ListenableFuture mockFuture = mock(ListenableFuture.class);
        when(processor.apply(any(MetricsCollection.class))).thenReturn(mockFuture);
        when(mockFuture.get(anyLong(), any(TimeUnit.class))).thenReturn(new ArrayList<Boolean>());
    }

    @Before
    public void buildPayload() throws IOException {

        payloadJson = getJsonFromFile("sample_payload.json", postfix);
    }
    
    @Test(expected = NumberFormatException.class)
    public void testExpectedGsonConversionFailure() {
        new LazilyParsedNumber("2.321").longValue();
    }
    
    @Test
    public void testGsonNumberConversions() {
        AggregatedPayload payload = AggregatedPayload.create(payloadJson);
        Number doubleNum = new LazilyParsedNumber("2.321");
        assertEquals( Double.parseDouble( "2.321" ), PreaggregateConversions.resolveNumber( doubleNum ) );
        
        Number longNum = new LazilyParsedNumber("12345");
        assertEquals(Long.parseLong("12345"), PreaggregateConversions.resolveNumber(longNum));
    }
    
    @Test
    public void testCounters() {
        AggregatedPayload payload = AggregatedPayload.create(payloadJson);
        Collection<PreaggregatedMetric> counters = PreaggregateConversions.convertCounters("1", 1, 15000, payload.getCounters());
        assertEquals( 6, counters.size() );
        ensureSerializability(counters);
    }
    
    @Test
    public void testGauges() {
        AggregatedPayload payload = AggregatedPayload.create(payloadJson);
        Collection<PreaggregatedMetric> gauges = PreaggregateConversions.convertGauges("1", 1, payload.getGauges());
        assertEquals( 4, gauges.size() );
        ensureSerializability(gauges);
    }
     
    @Test
    public void testSets() {
        AggregatedPayload payload = AggregatedPayload.create(payloadJson);
        Collection<PreaggregatedMetric> sets = PreaggregateConversions.convertSets("1", 1, payload.getSets());
        assertEquals( 2, sets.size() );
        ensureSerializability(sets);
    }
    
    @Test
    public void testTimers() {
        AggregatedPayload payload = AggregatedPayload.create(payloadJson);
        Collection<PreaggregatedMetric> timers = PreaggregateConversions.convertTimers("1", 1, payload.getTimers());
        assertEquals( 4, timers.size() );
        ensureSerializability(timers);
    }

    @Test
    public void testEnums() {
        AggregatedPayload payload = AggregatedPayload.create(payloadJson);
        Collection<PreaggregatedMetric> enums = PreaggregateConversions.convertEnums("1", 1, payload.getEnums());
        assertEquals( 1, enums.size() );
        ensureSerializability(enums);
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
    public void testEmptyJsonRequest() throws IOException {
        String requestBody = "{}"; //causes JsonMappingException
        FullHttpRequest request = createIngestRequest(requestBody);

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 3, errorResponse.getErrors().size());
        assertEquals("Invalid tenant", "", errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testEmptyTenantId() throws IOException {

        BluefloodGauge gauge = new BluefloodGauge("gauge.a.b", 5);
        FullHttpRequest request = createIngestRequest(createRequestBody("",
                new DefaultClockImpl().now().getMillis(), 0 , new BluefloodGauge[]{gauge}, null, null, null, null));

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "may not be empty", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid source", "tenantId", errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid tenant", "", errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid metric name", "", errorResponse.getErrors().get(0).getMetricName());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testInvalidFlushInterval() throws IOException {

        BluefloodGauge gauge = new BluefloodGauge("gauge.a.b", 5);
        FullHttpRequest request = createIngestRequest(createRequestBody(TENANT,
                new DefaultClockImpl().now().getMillis(), -1 , new BluefloodGauge[]{gauge}, null, null, null, null));

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "must be between 0 and 9223372036854775807", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid source", "flushInterval", errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid metric name", "", errorResponse.getErrors().get(0).getMetricName());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testCollectionTimeInPast() throws IOException {

        BluefloodGauge gauge = new BluefloodGauge("gauge.a.b", 5);
        long collectionTimeInPast = new DefaultClockImpl().now().getMillis() - 1000
                - Configuration.getInstance().getLongProperty( CoreConfig.BEFORE_CURRENT_COLLECTIONTIME_MS );

        FullHttpRequest request = createIngestRequest(createRequestBody(TENANT,
                collectionTimeInPast, 0 , new BluefloodGauge[]{gauge}, null, null, null, null));

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "Out of bounds. Cannot be more than 259200000 milliseconds into the past. " +
                "Cannot be more than 600000 milliseconds into the future", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid source", "timestamp", errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid metric name", "", errorResponse.getErrors().get(0).getMetricName());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testCollectionTimeInFuture() throws IOException {

        BluefloodGauge gauge = new BluefloodGauge("gauge.a.b", 5);
        long collectionTimeInFuture = new DefaultClockImpl().now().getMillis() + 1000
                + Configuration.getInstance().getLongProperty( CoreConfig.AFTER_CURRENT_COLLECTIONTIME_MS );

        FullHttpRequest request = createIngestRequest(createRequestBody(TENANT,
                collectionTimeInFuture, 0 , new BluefloodGauge[]{gauge}, null, null, null, null));

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "Out of bounds. Cannot be more than 259200000 milliseconds into the past. " +
                "Cannot be more than 600000 milliseconds into the future", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid source", "timestamp", errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid metric name", "", errorResponse.getErrors().get(0).getMetricName());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testAggregatedMetricsNotSet() throws IOException {

        FullHttpRequest request = createIngestRequest(createRequestBody(TENANT,
                new DefaultClockImpl().now().getMillis(), 0 , null, null, null, null, null));

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "At least one of the aggregated metrics(gauges, counters, timers, sets) " +
                "are expected", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid source", "", errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid metric name", "", errorResponse.getErrors().get(0).getMetricName());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testGaugeEmptyMetricName() throws IOException {

        BluefloodGauge gauge = new BluefloodGauge("", 5);
        FullHttpRequest request = createIngestRequest(createRequestBody(TENANT,
                new DefaultClockImpl().now().getMillis(), 0, new BluefloodGauge[]{gauge}, null, null, null, null));

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "may not be empty", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid source", "gauges[0].name", errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testGaugeEmptyMetricValue() throws IOException {

        String metricName = "gauge.a.b";
        BluefloodGauge gauge = new BluefloodGauge(metricName, null);
        FullHttpRequest request = createIngestRequest(createRequestBody(TENANT,
                new DefaultClockImpl().now().getMillis(), 0, new BluefloodGauge[]{gauge}, null, null, null, null));

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "may not be null", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid source", "gauges[0].value", errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid metric name", metricName, errorResponse.getErrors().get(0).getMetricName());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }


    @Test
    public void testCounterEmptyMetricName() throws IOException {

        BluefloodCounter counter = new BluefloodCounter("", 5, 0.1);
        FullHttpRequest request = createIngestRequest(createRequestBody(TENANT,
                new DefaultClockImpl().now().getMillis(), 0, null, new BluefloodCounter[]{counter}, null, null, null));

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "may not be empty", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid source", "counters[0].name", errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testCounterEmptyMetricValue() throws IOException {

        String metricName = "counter.a.b";
        BluefloodCounter counter = new BluefloodCounter(metricName, null, 0.1);
        FullHttpRequest request = createIngestRequest(createRequestBody(TENANT,
                new DefaultClockImpl().now().getMillis(), 0, null, new BluefloodCounter[]{counter}, null, null, null));

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "may not be null", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid source", "counters[0].value", errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid metric name", metricName, errorResponse.getErrors().get(0).getMetricName());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testCounterEmptyMetricRate() throws IOException {

        String metricName = "counter.a.b";
        BluefloodCounter counter = new BluefloodCounter(metricName, 5, null);
        FullHttpRequest request = createIngestRequest(createRequestBody(TENANT,
                new DefaultClockImpl().now().getMillis(), 0, null, new BluefloodCounter[]{counter}, null, null, null));

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "may not be null", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid source", "counters[0].rate", errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid metric name", metricName, errorResponse.getErrors().get(0).getMetricName());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }



    @Test
    public void testTimerEmptyMetricName() throws IOException {

        BluefloodTimer timer = new BluefloodTimer("", 5);
        FullHttpRequest request = createIngestRequest(createRequestBody(TENANT,
                new DefaultClockImpl().now().getMillis(), 0, null, null, new BluefloodTimer[]{timer}, null, null));

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "may not be empty", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid source", "timers[0].name", errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testTimerEmptyMetricCount() throws IOException {

        String metricName = "timer.a.b";
        BluefloodTimer timer = new BluefloodTimer(metricName, null);
        FullHttpRequest request = createIngestRequest(createRequestBody(TENANT,
                new DefaultClockImpl().now().getMillis(), 0, null, null, new BluefloodTimer[]{timer}, null, null));

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "may not be null", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid source", "timers[0].count", errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid metric name", metricName, errorResponse.getErrors().get(0).getMetricName());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testSetEmptyMetricName() throws IOException {

        BluefloodSet sets = new BluefloodSet("", new String[]{});
        FullHttpRequest request = createIngestRequest(createRequestBody(TENANT,
                new DefaultClockImpl().now().getMillis(), 0, null, null, null, new BluefloodSet[]{sets}, null));

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "may not be empty", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid source", "sets[0].name", errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testValidGauge() throws IOException {

        BluefloodGauge gauge = new BluefloodGauge("gauge.a.b", 5);
        FullHttpRequest request = createIngestRequest(createRequestBody(TENANT,
                new DefaultClockImpl().now().getMillis(), 0, new BluefloodGauge[]{gauge}, null, null, null, null));

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String responseBody = argument.getValue().content().toString(Charset.defaultCharset());

        assertEquals("Invalid response", "", responseBody);
        assertEquals("Invalid status", HttpResponseStatus.OK, argument.getValue().getStatus());
    }

    @Test
    public void testValidCounter() throws IOException {

        BluefloodCounter counter = new BluefloodCounter("counter.a.b", 5, 0.1);
        FullHttpRequest request = createIngestRequest(createRequestBody(TENANT,
                new DefaultClockImpl().now().getMillis(), 0, null, new BluefloodCounter[]{counter}, null, null, null));

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String responseBody = argument.getValue().content().toString(Charset.defaultCharset());

        assertEquals("Invalid response", "", responseBody);
        assertEquals("Invalid status", HttpResponseStatus.OK, argument.getValue().getStatus());
    }

    @Test
    public void testValidTimer() throws IOException {

        BluefloodTimer timer = new BluefloodTimer("timer.a.b", 5);
        FullHttpRequest request = createIngestRequest(createRequestBody(TENANT,
                new DefaultClockImpl().now().getMillis(), 0, null, null, new BluefloodTimer[]{timer}, null, null));

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String responseBody = argument.getValue().content().toString(Charset.defaultCharset());

        assertEquals("Invalid response", "", responseBody);
        assertEquals("Invalid status", HttpResponseStatus.OK, argument.getValue().getStatus());
    }

    @Test
    public void testValidSet() throws IOException {

        BluefloodSet set = new BluefloodSet("set.a.b", new String[]{"", ""});;
        FullHttpRequest request = createIngestRequest(createRequestBody(TENANT,
                new DefaultClockImpl().now().getMillis(), 0, null, null, null, new BluefloodSet[]{set}, null));

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String responseBody = argument.getValue().content().toString(Charset.defaultCharset());

        assertEquals("Invalid response", "", responseBody);
        assertEquals("Invalid status", HttpResponseStatus.OK, argument.getValue().getStatus());
    }

    private String createRequestBody(String tenantId, long collectionTime, long flushInterval, BluefloodGauge[] gauges,
                                     BluefloodCounter[] counters, BluefloodTimer[] timers, BluefloodSet[] sets,
                                     BluefloodEnum[] enums) {

        AggregatedPayload payload = new AggregatedPayload(tenantId, collectionTime, flushInterval,
                gauges, counters, timers, sets, enums);

        return new Gson().toJson(payload, AggregatedPayload.class);
    }


    private FullHttpRequest createIngestRequest(String requestBody) {
        return super.createPostRequest("/v2.0/" + TENANT + "/aggregated/", requestBody);
    }
}
