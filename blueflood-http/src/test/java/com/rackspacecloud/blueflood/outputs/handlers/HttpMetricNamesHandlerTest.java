package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.io.MetricName;
import com.rackspacecloud.blueflood.outputs.formats.ErrorResponse;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import junit.framework.Assert;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.nio.charset.Charset;
import java.util.*;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class HttpMetricNamesHandlerTest extends HandlerTestsBase {

    private DiscoveryIO mockDiscoveryHandle = mock(DiscoveryIO.class);
    private ChannelHandlerContext context;
    private Channel channel;
    private ChannelFuture channelFuture;

    private HttpMetricNamesHandler handler;

    @Before
    public void setup() {
        handler = new HttpMetricNamesHandler(mockDiscoveryHandle);

        channel = mock(Channel.class);
        channelFuture = mock(ChannelFuture.class);
        context = mock(ChannelHandlerContext.class);
        when(context.channel()).thenReturn(channel);
        when(channel.write(anyString())).thenReturn(channelFuture);
    }


    @Test
    public void emptyPrefix() throws Exception {
        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, createGetRequest("/v2.0/" + TENANT + "/metric_name/search"));
        verify(channel).write(argument.capture());
        verify(mockDiscoveryHandle, never()).getMetricNames(anyString(), anyString());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "Invalid Query String", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }

    @Test
    public void invalidQuerySize() throws Exception {
        ArgumentCaptor<FullHttpResponse> argument = ArgumentCaptor.forClass(FullHttpResponse.class);
        handler.handle(context, createGetRequest("/v2.0/" + TENANT + "/metric_name/search?query=foo&query=bar"));
        verify(channel).write(argument.capture());
        verify(mockDiscoveryHandle, never()).getMetricNames(anyString(), anyString());

        String errorResponseBody = argument.getValue().content().toString(Charset.defaultCharset());
        ErrorResponse errorResponse = getErrorResponse(errorResponseBody);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "Invalid Query String", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid tenant", TENANT, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST, argument.getValue().getStatus());
    }


    @Test
    public void validQuery() throws Exception {
        handler.handle(context, createGetRequest("/v2.0/" + TENANT + "/metric_name/search?query=foo"));
        verify(mockDiscoveryHandle, times(1)).getMetricNames(anyString(), anyString());
    }

    @Test
    public void testOutput() throws ParseException {
        List<MetricName> inputMetricNames = new ArrayList<MetricName>() {{
            add(new MetricName("foo", false));
            add(new MetricName("bar", false));
        }};

        String output = handler.getSerializedJSON(inputMetricNames);
        JSONParser jsonParser = new JSONParser();
        JSONArray tokenInfos = (JSONArray) jsonParser.parse(output);

        Assert.assertEquals("Unexpected result size", 2, tokenInfos.size());

        Set<String> expectedOutputSet = new HashSet<String>();
        for (MetricName metricName : inputMetricNames) {
            expectedOutputSet.add(metricName.getName() + "|" + metricName.isCompleteName());
        }
        Set<String> outputSet = new HashSet<String>();
        for (int i = 0; i< inputMetricNames.size(); i++) {
            JSONObject object = (JSONObject) tokenInfos.get(i);

            Iterator it = object.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry entry = (Map.Entry) it.next();
                outputSet.add(entry.getKey() + "|" + entry.getValue());
            }
        }

        Assert.assertEquals("Unexpected size", expectedOutputSet.size(), outputSet.size());
        Assert.assertTrue("Output contains no more elements than expected", expectedOutputSet.containsAll(outputSet));
        Assert.assertTrue("Output contains no less elements than expected", outputSet.containsAll(expectedOutputSet));
    }

    @Test
    public void testEmptyOutput() throws ParseException {
        List<MetricName> inputMetricNames = new ArrayList<MetricName>();

        String output = handler.getSerializedJSON(inputMetricNames);
        JSONParser jsonParser = new JSONParser();
        JSONArray tokenInfos = (JSONArray) jsonParser.parse(output);

        Assert.assertEquals("Unexpected result size", 0, tokenInfos.size());

    }
}
