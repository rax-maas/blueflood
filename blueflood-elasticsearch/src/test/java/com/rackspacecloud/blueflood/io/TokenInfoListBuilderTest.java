package com.rackspacecloud.blueflood.io;

import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TokenInfoListBuilderTest {

    @Test
    public void testSingleTokenWhichHasNextLevel() {
        TokenInfoListBuilder tokenListBuilder = new TokenInfoListBuilder();
        String token = "foo";
        tokenListBuilder.addTokenWithNextLevel(token);

        List<TokenInfo> resultList = tokenListBuilder.build();
        Assert.assertEquals("result size", 1, resultList.size());
        Assert.assertEquals("result token value", token, resultList.get(0).getToken());
        Assert.assertEquals("token's isNextLevel value", true, resultList.get(0).isNextLevel());

    }

    @Test
    public void testMultipleTokensWhichHasNextLevel() {
        TokenInfoListBuilder tokenListBuilder = new TokenInfoListBuilder();
        Set<String> expectedTokens = new HashSet<String>() {{
            add("foo");
            add("bar");
        }};
        tokenListBuilder.addTokenWithNextLevel(expectedTokens);

        List<TokenInfo> resultList = tokenListBuilder.build();
        Assert.assertEquals("result size", 2, resultList.size());
        
        Set<String> outputTokens = new HashSet<String>();
        for (TokenInfo tokenInfo: resultList) {
            outputTokens.add(tokenInfo.getToken());
            Assert.assertEquals("token's isNextLevel value", true, tokenInfo.isNextLevel());    
        }
        
        Assert.assertTrue("outputTokens should not have more than expected", expectedTokens.containsAll(outputTokens));
        Assert.assertTrue("all outputTokens should be in the expected", outputTokens.containsAll(expectedTokens));
        

    }

    @Test
    public void testAddingEnumValues() {
        TokenInfoListBuilder tokenListBuilder = new TokenInfoListBuilder();
        final String metricName = "foo.bar.baz";
        final String enumValue = "ev1";
        tokenListBuilder.addEnumValues(metricName, new ArrayList<String>() {{
            add(enumValue);
        }});

        List<TokenInfo> resultList = tokenListBuilder.build();
        Assert.assertEquals("result size", 1, resultList.size());
        Assert.assertEquals("result value", metricName + "." + enumValue, resultList.get(0).getToken());
    }

    @Test
    public void testAddingSingleToken() {
        TokenInfoListBuilder tokenListBuilder = new TokenInfoListBuilder();
        String token = "foo";
        tokenListBuilder.addToken(token, false);

        List<TokenInfo> resultList = tokenListBuilder.build();
        Assert.assertEquals("result size", 1, resultList.size());
        Assert.assertEquals("result value", token, resultList.get(0).getToken());
        Assert.assertEquals("token's isNextLevel value", false, resultList.get(0).isNextLevel());
    }
}
