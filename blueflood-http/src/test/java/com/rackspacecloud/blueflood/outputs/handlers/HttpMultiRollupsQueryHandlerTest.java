package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.outputs.formats.ErrorResponse;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.outputs.serializers.BasicRollupsOutputSerializer;
import com.rackspacecloud.blueflood.outputs.serializers.BatchedMetricsJSONOutputSerializer;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.HttpConfig;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.json.simple.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HttpMultiRollupsQueryHandlerTest extends HandlerTestsBase {

    private HttpMultiRollupsQueryHandler handler;

    private static final String TENANT = "tenant";

    private ChannelHandlerContext context;
    private Channel channel;
    private BatchedMetricsJSONOutputSerializer serializer;

    @Before
    public void setup() {
        serializer = mock(BatchedMetricsJSONOutputSerializer.class);
        handler = new HttpMultiRollupsQueryHandler(serializer);

        channel = mock(Channel.class);
        context = mock(ChannelHandlerContext.class);
        when(context.channel()).thenReturn(channel);
    }

    @Test
    public void testWithNoRequestBody() throws IOException {
        FullHttpRequest request = createQueryRequest("", "");

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "Invalid body. Expected JSON array of metrics.", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testWithTooMayLocatorsInRequestBody() throws IOException {
        int maxLimit = Configuration.getInstance().getIntegerProperty(HttpConfig.MAX_METRICS_PER_BATCH_QUERY);
        FullHttpRequest request = createQueryRequest("",
                createRequestBody(maxLimit + 1));

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "Too many metrics fetch in a single call. Max limit is " + maxLimit + ".",
                errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testWithNoQueryParams() throws IOException {
        FullHttpRequest request = createQueryRequest("", createRequestBody(1));

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "No query parameters present.", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testMissingRequiredQueryParams() throws IOException {
        FullHttpRequest request = createQueryRequest("?from=111111", createRequestBody(1));

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "Either 'points' or 'resolution' is required.", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void testRequestWithSerializationException() throws IOException {
        FullHttpRequest request = createQueryRequest("?points=10&from=1&to=2", createRequestBody(1));

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        String message = "mock exception message";
        when(serializer.transformRollupData(anyMap(), anySet())).thenThrow(new SerializationException(message));
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", message, errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.INTERNAL_SERVER_ERROR, argument.getValue().getStatus());
    }

    private String createRequestBody(int batchSize) {

        StringBuilder sb = new StringBuilder();
        sb.append("[");

        for (int i =0; i < batchSize; i ++) {
            sb.append("\'metric" + i + "\'");
            sb.append(",");
        }

        sb.deleteCharAt(sb.length() - 1);
        sb.append("]");

        return sb.toString();
    }

    private FullHttpRequest createQueryRequest(String queryParams, String requestBody) {
        return super.createPostRequest("/v2.0/" + TENANT + "/views/" + queryParams, requestBody);
    }
}
