package com.rackspacecloud.blueflood.inputs.handlers;

import com.rackspacecloud.blueflood.inputs.formats.JSONMetricScoped;
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
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.common.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class HttpMultitenantMetricsIngestionHandlerTest extends HandlerTestsBase {

    private HttpMultitenantMetricsIngestionHandler handler;
    private HttpMetricsIngestionServer.Processor processor;

    private ChannelHandlerContext context;
    private Channel channel;
    private ChannelFuture channelFuture;

    private static final String TENANT = "tenant";

    @Before
    public void setup() {
        processor = mock(HttpMetricsIngestionServer.Processor.class);
        handler = new HttpMultitenantMetricsIngestionHandler(processor, new TimeValue(5, TimeUnit.SECONDS));

        channel = mock(Channel.class);
        context = mock(ChannelHandlerContext.class);
        channelFuture = mock(ChannelFuture.class);
        when(context.channel()).thenReturn(channel);
        when(channel.write(anyString())).thenReturn(channelFuture);
    }

    @Test
    public void testMultiMetricsEmptyArrays() throws IOException {
        String requestBody = "[]";
        FullHttpRequest request = createRequest(requestBody);

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid metric name", "", errorResponse.getErrors().get(0).getMetricName());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
        assertEquals("Invalid error source", null, errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid error message", "No valid metrics", errorResponse.getErrors().get(0).getMessage());
    }

    @Test
    public void testSingleMetricEmptyTenantId() throws IOException {
        String metricName = "a.b.c";
        String singleMetric = createRequestBody("", metricName, new DefaultClockImpl().now().getMillis(),
                24 * 60 * 60, 1); //empty metric name

        String requestBody = "[" + singleMetric + "]";
        FullHttpRequest request = createRequest(requestBody);

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid tenant", "", errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid metric name", metricName, errorResponse.getErrors().get(0).getMetricName());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
        assertEquals("Invalid error source", "tenantId", errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid error message", "may not be empty", errorResponse.getErrors().get(0).getMessage());
    }


    @Test
    public void testMultiMetricsInvalidRequest() throws IOException {
        final String TENANT1 = "tenant1";
        final String TENANT2 = "tenant2";

        final String metricName1 = "a.b.c.1";
        final String metricName2 = "a.b.c.2";
        long collectionTimeInPast = new DefaultClockImpl().now().getMillis() - 1000
                - Configuration.getInstance().getLongProperty( CoreConfig.BEFORE_CURRENT_COLLECTIONTIME_MS );

        String singleMetric1 = createRequestBody(TENANT1, metricName1, new DefaultClockImpl().now().getMillis(),
                -1, 1); //invalid ttl
        String singleMetric2 = createRequestBody(TENANT2, metricName2, collectionTimeInPast, 24 * 60 * 60, 1); //collection in past

        String requestBody = "[" + singleMetric1 + "," + singleMetric2 + "]";
        FullHttpRequest request = createRequest(requestBody);

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 2, errorResponse.getErrors().size());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());

        assertEquals("Invalid tenant", TENANT1, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid tenant", metricName1, errorResponse.getErrors().get(0).getMetricName());
        assertEquals("Invalid error source", "ttlInSeconds", errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid error message", "must be between 1 and 2147483647", errorResponse.getErrors().get(0).getMessage());

        assertEquals("Invalid tenant", TENANT2, errorResponse.getErrors().get(1).getTenantId());
        assertEquals("Invalid tenant", metricName2, errorResponse.getErrors().get(1).getMetricName());
        assertEquals("Invalid error source", "collectionTime", errorResponse.getErrors().get(1).getSource());
        assertEquals("Invalid error message", "Out of bounds. Cannot be more than 259200000 milliseconds into the past." +
                " Cannot be more than 600000 milliseconds into the future", errorResponse.getErrors().get(1).getMessage());

    }

    private String createRequestBody(String tenantId, String metricName, long collectionTime, int ttl, Object metricValue) throws IOException {
        JSONMetricScoped metric = new JSONMetricScoped();
        if (!StringUtils.isEmpty(tenantId))
            metric.setTenantId(tenantId);
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

    private FullHttpRequest createRequest(String requestBody) {
        return super.createPostRequest("/v2.0/" + TENANT + "/ingest/multi", requestBody);
    }

}
