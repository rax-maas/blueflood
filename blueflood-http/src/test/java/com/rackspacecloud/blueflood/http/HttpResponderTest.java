package com.rackspacecloud.blueflood.http;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 *
 */
public class HttpResponderTest {

    @Test
    public void testDefaultHttpConnIdleTimeout_RequestKeepAlive_ShouldHaveResponseKeepAlive() {
        HttpResponder responder = new HttpResponder();

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        FullHttpRequest request = mock(FullHttpRequest.class);
        FullHttpResponse response = mock(FullHttpResponse.class);
        HttpHeaders headers = mock(HttpHeaders.class);
        Channel channel = mock(Channel.class);

        when(ctx.channel()).thenReturn(channel);

        when(request.content()).thenReturn(null);
        when(request.headers()).thenReturn(headers);
        when(headers.get(HttpHeaders.Names.CONNECTION)).thenReturn(HttpHeaders.Values.KEEP_ALIVE);
        when(request.getProtocolVersion()).thenReturn(HttpVersion.HTTP_1_1);

        HttpHeaders responseHeaders = new DefaultHttpHeaders();
        when(response.headers()).thenReturn(responseHeaders);

        responder.respond(ctx, request, response);

        assertEquals("Connection: response header", HttpHeaders.Values.KEEP_ALIVE, responseHeaders.get(HttpHeaders.Names.CONNECTION));
    }

    @Test
    public void testNonZeroHttpConnIdleTimeout_RequestKeepAlive_ShouldHaveResponseKeepAliveTimeout() {
        int idleTimeout = 300;
        HttpResponder responder = new HttpResponder(idleTimeout);

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        FullHttpRequest request = mock(FullHttpRequest.class);
        FullHttpResponse response = mock(FullHttpResponse.class);
        HttpHeaders headers = mock(HttpHeaders.class);
        Channel channel = mock(Channel.class);
        ChannelFuture future = mock(ChannelFuture.class);

        when(ctx.channel()).thenReturn(channel);
        when(ctx.writeAndFlush(any())).thenReturn(future);

        when(request.content()).thenReturn(null);
        when(request.headers()).thenReturn(headers);
        when(headers.get(HttpHeaders.Names.CONNECTION)).thenReturn(HttpHeaders.Values.KEEP_ALIVE);
        when(request.getProtocolVersion()).thenReturn(HttpVersion.HTTP_1_1);

        HttpHeaders responseHeaders = new DefaultHttpHeaders();
        when(response.headers()).thenReturn(responseHeaders);

        responder.respond(ctx, request, response);

        assertEquals("Connection: response header", HttpHeaders.Values.KEEP_ALIVE, responseHeaders.get(HttpHeaders.Names.CONNECTION));
        assertEquals("Keep-Alive: response header", "timeout="+idleTimeout, responseHeaders.get("Keep-Alive"));
    }
}
