package com.rackspacecloud.blueflood.io;

import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MetricTokenListBuilderTest {

    @Test
    public void testSingleTokenPathWhichHasNextLevel() {
        MetricTokenListBuilder tokenListBuilder = new MetricTokenListBuilder();
        String token = "foo";
        tokenListBuilder.addTokenPathWithNextLevel(token);

        List<MetricToken> resultList = tokenListBuilder.build();
        Assert.assertEquals("result size", 1, resultList.size());
        Assert.assertEquals("result token value", token, resultList.get(0).getPath());
        Assert.assertEquals("token's isLeaf value", false, resultList.get(0).isLeaf());
        Assert.assertEquals("token value", token, resultList.get(0).getToken());

    }

    @Test
    public void testMultipleTokenPathsWhichHasNextLevel() {
        MetricTokenListBuilder tokenListBuilder = new MetricTokenListBuilder();
        Set<String> expectedTokens = new HashSet<String>() {{
            add("foo");
            add("bar");
        }};
        tokenListBuilder.addTokenPathWithNextLevel(expectedTokens);

        List<MetricToken> resultList = tokenListBuilder.build();
        Assert.assertEquals("result size", 2, resultList.size());
        
        Set<String> outputTokens = new HashSet<String>();
        for (MetricToken metricToken : resultList) {
            outputTokens.add(metricToken.getPath());
            Assert.assertEquals("token's isLeaf value", false, metricToken.isLeaf());
        }
        
        Assert.assertTrue("outputTokens should not have more than expected", expectedTokens.containsAll(outputTokens));
        Assert.assertTrue("all outputTokens should be in the expected", outputTokens.containsAll(expectedTokens));
        

    }

    @Test
    public void testAddingEnumValues() {
        MetricTokenListBuilder tokenListBuilder = new MetricTokenListBuilder();
        final String metricName = "foo.bar.baz";
        final String enumValue = "ev1";
        String metricNameWithEnumExtension = metricName + "." + enumValue;
        tokenListBuilder.addMetricNameWithEnumExtension(metricNameWithEnumExtension);

        List<MetricToken> resultList = tokenListBuilder.build();
        Assert.assertEquals("result size", 1, resultList.size());
        Assert.assertEquals("result value", metricNameWithEnumExtension, resultList.get(0).getPath());
        Assert.assertEquals("isLeaf result value", true, resultList.get(0).isLeaf());
        Assert.assertEquals("token value", enumValue, resultList.get(0).getToken());
    }

    @Test
    public void testAddingSingleToken() {
        MetricTokenListBuilder tokenListBuilder = new MetricTokenListBuilder();
        String token = "foo";
        tokenListBuilder.addTokenPath(token, false);

        List<MetricToken> resultList = tokenListBuilder.build();
        Assert.assertEquals("result size", 1, resultList.size());
        Assert.assertEquals("result value", token, resultList.get(0).getPath());
        Assert.assertEquals("token's isLeaf value", false, resultList.get(0).isLeaf());
    }
}
