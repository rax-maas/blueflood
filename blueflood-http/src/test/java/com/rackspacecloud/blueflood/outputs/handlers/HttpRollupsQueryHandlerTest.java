package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.outputs.formats.ErrorResponse;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.outputs.serializers.BasicRollupsOutputSerializer;
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

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class HttpRollupsQueryHandlerTest extends HandlerTestsBase {

    private HttpRollupsQueryHandler handler;

    private static final String TENANT = "tenant";

    private ChannelHandlerContext context;
    private Channel channel;
    private BasicRollupsOutputSerializer<JSONObject> serializer;

    @Before
    public void setup() {
        serializer = mock(BasicRollupsOutputSerializer.class);
        handler = new HttpRollupsQueryHandler(serializer);

        channel = mock(Channel.class);
        context = mock(ChannelHandlerContext.class);
        when(context.channel()).thenReturn(channel);
    }

    @Test
    public void testWithNoQueryParams() throws IOException {
        FullHttpRequest request = createQueryRequest("");

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
        FullHttpRequest request = createQueryRequest("?from=111111");

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
        FullHttpRequest request = createQueryRequest("?points=10&from=1&to=2");

        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        String message = "mock exception message";
        when(serializer.transformRollupData(any(MetricData.class), anySet())).thenThrow(new SerializationException(message));
        handler.handle(context, request);
        verify(channel).write(argument.capture());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", message, errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.INTERNAL_SERVER_ERROR, argument.getValue().getStatus());
    }

    private FullHttpRequest createQueryRequest(String queryParams) {
        return super.createGetRequest("/v2.0/" + TENANT + "/views/" + queryParams);
    }

}
