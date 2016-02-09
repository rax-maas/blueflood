package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.io.TokenInfo;
import junit.framework.Assert;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.SucceededChannelFuture;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class HttpMetricNameTokensHandlerTest extends BaseHandlerTest {

    private DiscoveryIO mockDiscoveryHandle = mock(DiscoveryIO.class);
    private ChannelHandlerContext context;
    private Channel channel;

    private HttpMetricNameTokensHandler handler;

    @Before
    public void setup() {
        handler = new HttpMetricNameTokensHandler(mockDiscoveryHandle);

        channel = mock(Channel.class);
        context = mock(ChannelHandlerContext.class);
        when(context.getChannel()).thenReturn(channel);
        when(channel.write(anyString())).thenReturn(new SucceededChannelFuture(channel));
    }


    @Test
    public void emptyPrefix() throws Exception {
        handler.handle(context, createGetRequest("/v2.0/" + TENANT + "/next_token"));
        verify(mockDiscoveryHandle, times(1)).getNextTokens(anyString(), anyString());
    }

    @Test
    public void invalidPrefixSize() throws Exception {
        handler.handle(context, createGetRequest("/v2.0/" + TENANT + "/next_token?prefix=foo&prefix=bar"));
        verify(mockDiscoveryHandle, never()).getNextTokens(anyString(), anyString());
    }


    @Test
    public void validPrefix() throws Exception {
        handler.handle(context, createGetRequest("/v2.0/" + TENANT + "/next_token?prefix=foo"));
        verify(mockDiscoveryHandle, times(1)).getNextTokens(anyString(), anyString());
    }

    @Test
    public void testOutput() throws ParseException {
        List<TokenInfo> inputTokenInfos = new ArrayList<TokenInfo>() {{
            add(new TokenInfo("foo", true));
            add(new TokenInfo("bar", true));
        }};

        String output = handler.getSerializedJSON(inputTokenInfos);
        JSONParser jsonParser = new JSONParser();
        JSONArray tokenInfos = (JSONArray) jsonParser.parse(output);

        Assert.assertEquals("Unexpected result size", 2, tokenInfos.size());

        Set<String> expectedOutputSet = new HashSet<String>();
        for (TokenInfo tokenInfo: inputTokenInfos) {
            expectedOutputSet.add(tokenInfo.getToken() + "|" + tokenInfo.isNextLevel());
        }
        Set<String> outputSet = new HashSet<String>();
        for (int i = 0; i< inputTokenInfos.size(); i++) {
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
        List<TokenInfo> inputTokenInfos = new ArrayList<TokenInfo>();

        String output = handler.getSerializedJSON(inputTokenInfos);
        JSONParser jsonParser = new JSONParser();
        JSONArray tokenInfos = (JSONArray) jsonParser.parse(output);

        Assert.assertEquals("Unexpected result size", 0, tokenInfos.size());

    }
}
