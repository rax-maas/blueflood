package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.outputs.formats.ErrorResponse;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.charset.Charset;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HttpMetricsIndexHandlerTest extends HandlerTestsBase {

    private HttpMetricsIndexHandler handler;

    private static final String TENANT = "tenant";

    private ChannelHandlerContext context;
    private Channel channel;

    @Before
    public void setup() {
        handler = new HttpMetricsIndexHandler();

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
        assertEquals("Invalid error message", "Invalid Query String", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    private FullHttpRequest createQueryRequest(String queryParams) {
        return super.createGetRequest("/v2.0/" + TENANT + "/search/" + queryParams);
    }
}
