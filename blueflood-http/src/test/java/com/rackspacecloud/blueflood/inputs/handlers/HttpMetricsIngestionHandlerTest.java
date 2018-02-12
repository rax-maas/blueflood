package com.rackspacecloud.blueflood.inputs.handlers;

import com.codahale.metrics.Meter;
import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.inputs.formats.JSONMetric;
import com.rackspacecloud.blueflood.inputs.formats.JSONMetricsContainer;
import com.rackspacecloud.blueflood.io.Instrumentation;
import com.rackspacecloud.blueflood.outputs.formats.ErrorResponse;
import com.rackspacecloud.blueflood.outputs.handlers.HandlerTestsBase;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.utils.DefaultClockImpl;
import com.rackspacecloud.blueflood.utils.TimeValue;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class HttpMetricsIngestionHandlerTest extends HandlerTestsBase {

    private HttpMetricsIngestionHandler handler;
    private HttpMetricsIngestionServer.Processor processor;

    private ChannelHandlerContext context;
    private Channel channel;
    private ChannelFuture channelFuture;

    private Meter ingestedMetrics;
    private Meter ingestedDelayedMetrics;

    private static final String TENANT = "tenant";

    @Before
    public void setup() {
        processor = mock(HttpMetricsIngestionServer.Processor.class);
        handler = new HttpMetricsIngestionHandler(processor, new TimeValue(5, TimeUnit.SECONDS));

        channel = mock(Channel.class);
        context = mock(ChannelHandlerContext.class);
        channelFuture = mock(ChannelFuture.class);
        when(context.channel()).thenReturn(channel);
        when(channel.write(anyString())).thenReturn(channelFuture);

        ingestedMetrics = Instrumentation.getIngestedMetricsMeter(TENANT);
        ingestedDelayedMetrics = Instrumentation.getIngestedDelayedMetricsMeter(TENANT);
    }

    @Test
    public void emptyRequest_shouldGenerateErrorResponse() throws IOException {
        String requestBody = "";
        FullHttpRequest request = createIngestRequest(requestBody);

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "Cannot parse content", errorResponse.getErrors().get(0).getMessage());
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

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "Cannot parse content", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testInvalidJsonRequest() throws IOException {
        String requestBody = "{\"xxxx\": yyyy}"; //causes JsonMappingException
        FullHttpRequest request = createIngestRequest(requestBody);

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "Cannot parse content", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testEmptyJsonArrayRequest() throws IOException {
        String requestBody = "[]"; //causes JsonMappingException
        FullHttpRequest request = createIngestRequest(requestBody);

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "No valid metrics",
                errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testEmptyMetricRequest() throws IOException {
        String requestBody = "[{}]"; //causes JsonMappingException
        FullHttpRequest request = createIngestRequest(requestBody);

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 3, errorResponse.getErrors().size());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testSingleMetricInvalidMetricName() throws IOException {
        String metricName = "";
        String singleMetric = createRequestBody(metricName, new DefaultClockImpl().now().getMillis(),
                24 * 60 * 60, 1); //empty metric name

        String requestBody = "[" + singleMetric + "]";
        FullHttpRequest request = createIngestRequest(requestBody);

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid metric name", metricName, errorResponse.getErrors().get(0).getMetricName());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
        assertEquals("Invalid error source", "metricName", errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid error message", "may not be empty", errorResponse.getErrors().get(0).getMessage());
    }


    @Test
    public void testSingleMetricCollectionTimeInPast() throws IOException {
        long collectionTimeInPast = new DefaultClockImpl().now().getMillis() - 1000
                - Configuration.getInstance().getLongProperty( CoreConfig.BEFORE_CURRENT_COLLECTIONTIME_MS );
        String metricName = "a.b.c";
        String singleMetric = createRequestBody(metricName, collectionTimeInPast, 24 * 60 * 60, 1); //collection in past

        String requestBody = "[" + singleMetric + "]";
        FullHttpRequest request = createIngestRequest(requestBody);

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid metric name", metricName, errorResponse.getErrors().get(0).getMetricName());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
        assertEquals("Invalid error source", "collectionTime", errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid error message", "Out of bounds. Cannot be more than 259200000 milliseconds into the past." +
                " Cannot be more than 600000 milliseconds into the future", errorResponse.getErrors().get(0).getMessage());
    }

    @Test
    public void testSingleMetricCollectionTimeInFuture() throws IOException {
        long collectionTimeInFuture = new DefaultClockImpl().now().getMillis() + 1000
                + Configuration.getInstance().getLongProperty( CoreConfig.AFTER_CURRENT_COLLECTIONTIME_MS );
        String metricName = "a.b.c";
        String singleMetric = createRequestBody(metricName, collectionTimeInFuture, 24 * 60 * 60, 1); //collection in future

        String requestBody = "[" + singleMetric + "]";
        FullHttpRequest request = createIngestRequest(requestBody);

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid metric name", metricName, errorResponse.getErrors().get(0).getMetricName());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
        assertEquals("Invalid error source", "collectionTime", errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid error message", "Out of bounds. Cannot be more than 259200000 milliseconds into the past." +
                " Cannot be more than 600000 milliseconds into the future", errorResponse.getErrors().get(0).getMessage());
    }

    @Test
    public void testSingleMetricInvalidTTL() throws IOException {
        String metricName = "a.b.c";
        String singleMetric = createRequestBody(metricName, new DefaultClockImpl().now().getMillis(), 0, 1); //ttl of 0
        String requestBody = "[" + singleMetric + "]";
        FullHttpRequest request = createIngestRequest(requestBody);

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid metric name", metricName, errorResponse.getErrors().get(0).getMetricName());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
        assertEquals("Invalid error source", "ttlInSeconds", errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid error message", "must be between 1 and 2147483647", errorResponse.getErrors().get(0).getMessage());
    }

    @Test
    public void testSingleMetricInvalidMetricValue() throws IOException {
        String metricName = "a.b.c";
        String singleMetric = createRequestBody(metricName, new DefaultClockImpl().now().getMillis(),
                24 * 60 * 60, null); //empty metric value

        String requestBody = "[" + singleMetric + "]";
        FullHttpRequest request = createIngestRequest(requestBody);

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);
        System.out.println(errorResponse);
        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid metric name", "", errorResponse.getErrors().get(0).getMetricName());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
        assertNull("Invalid error source", errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid error message", "No valid metrics", errorResponse.getErrors().get(0).getMessage());
    }

    @Test
    public void testMultiMetricsInvalidRequest() throws IOException {
        String metricName1 = "a.b.c.1";
        String metricName2 = "a.b.c.2";
        FullHttpRequest request = createIngestRequest(generateInvalidMetrics(metricName1, metricName2));

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 2, errorResponse.getErrors().size());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());

        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid tenant", metricName1, errorResponse.getErrors().get(0).getMetricName());
        assertEquals("Invalid error source", "ttlInSeconds", errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid error message", "must be between 1 and 2147483647", errorResponse.getErrors().get(0).getMessage());

        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(1).getTenantId());
        assertEquals("Invalid tenant", metricName2, errorResponse.getErrors().get(1).getMetricName());
        assertEquals("Invalid error source", "collectionTime", errorResponse.getErrors().get(1).getSource());
        assertEquals("Invalid error message", "Out of bounds. Cannot be more than 259200000 milliseconds into the past." +
                " Cannot be more than 600000 milliseconds into the future", errorResponse.getErrors().get(1).getMessage());
    }

    @Test
    public void perTenantMetricsOn_emptyRequest_shouldNotRecordAnything() throws IOException {
        String requestBody = "[{}]";
        FullHttpRequest request = createIngestRequest(requestBody);

        long ingestedMetricsBefore = ingestedMetrics.getCount();
        long ingestedDelayedMetricsBefore = ingestedDelayedMetrics.getCount();

        HttpMetricsIngestionHandler handler = spy(new HttpMetricsIngestionHandler(processor, new TimeValue(5, TimeUnit.SECONDS), true));
        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());
        verify(handler, never()).recordPerTenantMetrics(eq(TENANT), anyInt(), anyInt());

        assertEquals("ingested metrics count", 0, ingestedMetrics.getCount() - ingestedMetricsBefore);
        assertEquals("ingested delayed metrics count", 0, ingestedDelayedMetrics.getCount() - ingestedDelayedMetricsBefore);
    }

    @Test
    public void perTenantMetricsOn_invalidMetrics_shouldNotRecordAnything() throws IOException {

        String m1 = "foo.bar";
        String m2 = "gee.wish";
        FullHttpRequest request = createIngestRequest(generateInvalidMetrics(m1, m2));

        long ingestedMetricsBefore = ingestedMetrics.getCount();
        long ingestedDelayedMetricsBefore = ingestedDelayedMetrics.getCount();

        HttpMetricsIngestionHandler handler = spy(new HttpMetricsIngestionHandler(processor, new TimeValue(5, TimeUnit.SECONDS), true));
        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());
        verify(handler, never()).recordPerTenantMetrics(eq(TENANT), anyInt(), anyInt());

        assertEquals("ingested metrics count", 0, ingestedMetrics.getCount() - ingestedMetricsBefore);
        assertEquals("ingested delayed metrics count", 0, ingestedDelayedMetrics.getCount() - ingestedDelayedMetricsBefore);
    }

    @Test
    public void perTenantMetricsOn_shouldRecordDelayedMetrics() throws Exception {
        String delayedMetric1 = "delayed.me.1";
        String delayedMetric2 = "delayed.me.2";
        FullHttpRequest request = createIngestRequest(generateDelayedMetricsRequestString(delayedMetric1, delayedMetric2));

        long ingestedMetricsBefore = ingestedMetrics.getCount();
        long ingestedDelayedMetricsBefore = ingestedDelayedMetrics.getCount();

        ListenableFuture<List<Boolean>> futures = mock(ListenableFuture.class);
        List<Boolean> answers = new ArrayList<>();
        answers.add(Boolean.TRUE);
        when(processor.apply(any())).thenReturn(futures);
        when(futures.get(anyLong(), any())).thenReturn(answers);

        HttpMetricsIngestionHandler handler = spy(new HttpMetricsIngestionHandler(processor, new TimeValue(5, TimeUnit.SECONDS), true));
        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());
        verify(handler, times(1)).recordPerTenantMetrics(eq(TENANT), eq(0), eq(2));

        assertEquals("ingested metrics count", 0, ingestedMetrics.getCount() - ingestedMetricsBefore);
        assertEquals("ingested delayed metrics count", 2, ingestedDelayedMetrics.getCount() - ingestedDelayedMetricsBefore);
    }

    @Test
    public void perTenantMetricsOn_shouldRecordNonDelayedMetrics() throws Exception {
        String metric1 = "i.am.on.time";
        String metric2 = "i.am.on.time.again";
        FullHttpRequest request = createIngestRequest(generateNonDelayedMetricsRequestString(metric1, metric2));

        long ingestedMetricsBefore = ingestedMetrics.getCount();
        long ingestedDelayedMetricsBefore = ingestedDelayedMetrics.getCount();

        ListenableFuture<List<Boolean>> futures = mock(ListenableFuture.class);
        List<Boolean> answers = new ArrayList<>();
        answers.add(Boolean.TRUE);
        when(processor.apply(any())).thenReturn(futures);
        when(futures.get(anyLong(), any())).thenReturn(answers);

        HttpMetricsIngestionHandler handler = spy(new HttpMetricsIngestionHandler(processor, new TimeValue(5, TimeUnit.SECONDS), true));
        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);

        verify(channel).write(argument.capture());
        verify(handler, times(1)).recordPerTenantMetrics(eq(TENANT), eq(2), eq(0));

        assertEquals("ingested metrics count", 2, ingestedMetrics.getCount() - ingestedMetricsBefore);
        assertEquals("ingested delayed metrics count", 0, ingestedDelayedMetrics.getCount() - ingestedDelayedMetricsBefore);
    }

    @Test
    public void perTenantMetricsOff_shouldNotRecordMetrics() throws Exception {
        String metric1 = "i.am.on.time";
        String metric2 = "i.am.on.time.again";
        FullHttpRequest request = createIngestRequest(generateNonDelayedMetricsRequestString(metric1, metric2));

        long ingestedMetricsBefore = ingestedMetrics.getCount();
        long ingestedDelayedMetricsBefore = ingestedDelayedMetrics.getCount();

        ListenableFuture<List<Boolean>> futures = mock(ListenableFuture.class);
        List<Boolean> answers = new ArrayList<>();
        answers.add(Boolean.TRUE);
        when(processor.apply(any())).thenReturn(futures);
        when(futures.get(anyLong(), any())).thenReturn(answers);

        // turn off per tenant metrics tracking
        HttpMetricsIngestionHandler handler = spy(new HttpMetricsIngestionHandler(processor, new TimeValue(5, TimeUnit.SECONDS), false));
        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());
        verify(handler, times(1)).recordPerTenantMetrics(eq(TENANT), eq(2), eq(0));

        assertEquals("ingested metrics count", 0, ingestedMetrics.getCount() - ingestedMetricsBefore);
        assertEquals("ingested delayed metrics count", 0, ingestedDelayedMetrics.getCount() - ingestedDelayedMetricsBefore);

    }

    private String generateInvalidMetrics(String invalidTtlMetricName, String invalidCollectionMetricName) throws IOException {

        long collectionTimeInPast = new DefaultClockImpl().now().getMillis() - 1000
                - Configuration.getInstance().getLongProperty( CoreConfig.BEFORE_CURRENT_COLLECTIONTIME_MS );

        //invalid ttl value
        String invalidTtl = createRequestBody(invalidTtlMetricName, new DefaultClockImpl().now().getMillis(), -1, 1);

        // collection in the past
        String invalidCollection = createRequestBody(invalidCollectionMetricName, collectionTimeInPast, 24 * 60 * 60, 1);

        return "[" + invalidTtl + "," + invalidCollection + "]";
    }

    private String generateDelayedMetricsRequestString(String... metricNames) throws IOException {
        long delayedTime = new DefaultClockImpl().now().getMillis() - 100 -
                Configuration.getInstance().getLongProperty(CoreConfig.ROLLUP_DELAY_MILLIS);
        List<String> jsonMetrics = new ArrayList<>();
        for (String metricName : metricNames) {
            jsonMetrics.add(createRequestBody(metricName, delayedTime, 24*60*60, 1));
        }
        return "[" + StringUtils.join(jsonMetrics, ",") + "]";
    }

    private String generateNonDelayedMetricsRequestString(String... metricNames) throws IOException {
        long timestamp = new DefaultClockImpl().now().getMillis();
        List<String> jsonMetrics = new ArrayList<>();
        for (String metricName : metricNames) {
            jsonMetrics.add(createRequestBody(metricName, timestamp, 24*60*60, 1));
        }
        return "[" + StringUtils.join(jsonMetrics, ",") + "]";
    }

    private String createRequestBody(String metricName, long collectionTime, int ttl, Object metricValue) throws IOException {
        JSONMetric metric = new JSONMetric();
        if (!StringUtils.isEmpty(metricName))
            metric.setMetricName(metricName);
        if (collectionTime > 0)
            metric.setCollectionTime(collectionTime);
        if (ttl > 0)
            metric.setTtlInSeconds(ttl);
        if (metricValue != null)
            metric.setMetricValue(metricValue);

        return new ObjectMapper().writeValueAsString(metric);
    }

    private FullHttpRequest createIngestRequest(String requestBody) {
        return super.createPostRequest("/v2.0/" + TENANT + "/ingest/", requestBody);
    }


    public JSONMetricsContainer getContainer(String tenantId, String jsonBody) throws IOException {
        HttpMetricsIngestionHandler handler = new HttpMetricsIngestionHandler(null, new TimeValue(5, TimeUnit.SECONDS));
        return handler.createContainer(jsonBody, tenantId);
    }
}
