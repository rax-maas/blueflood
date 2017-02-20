package com.rackspacecloud.blueflood.io;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class MetricNameListBuilderTest {

    @Test
    public void testSingleMetricNameWhichHasNextLevel() {
        MetricNameListBuilder metricNameListBuilder = new MetricNameListBuilder();
        String metricName = "foo";
        metricNameListBuilder.addMetricNameWithNextLevel(metricName);

        List<MetricName> resultList = metricNameListBuilder.build();
        assertEquals("result size", 1, resultList.size());
        assertEquals("result metricName value", metricName, resultList.get(0).getName());
        assertEquals("metricName's isCompleteName value", false, resultList.get(0).isCompleteName());

    }

    @Test
    public void testMultipleMetricNamesWhichHasNextLevel() {
        MetricNameListBuilder metricNameListBuilder = new MetricNameListBuilder();
        Set<String> metricNames = new HashSet<String>() {{
            add("foo");
            add("bar");
        }};
        metricNameListBuilder.addMetricNameWithNextLevel(metricNames);

        List<MetricName> resultList = metricNameListBuilder.build();
        assertEquals("result size", 2, resultList.size());
        
        Set<String> outputMetricNames = new HashSet<String>();
        for (MetricName metricName : resultList) {
            outputMetricNames.add(metricName.getName());
            assertEquals("token's isCompleteName value", false, metricName.isCompleteName());
        }
        
        Assert.assertTrue("outputMetricNames should not have more than expected", metricNames.containsAll(outputMetricNames));
        Assert.assertTrue("all outputMetricNames should be in the expected", outputMetricNames.containsAll(metricNames));
    }

    @Test
    public void testAddingSingleCompleteMetricName() {
        MetricNameListBuilder tokenListBuilder = new MetricNameListBuilder();
        String metricName = "foo";
        tokenListBuilder.addCompleteMetricName(metricName);

        List<MetricName> resultList = tokenListBuilder.build();
        assertEquals("result size", 1, resultList.size());
        assertEquals("result value", metricName, resultList.get(0).getName());
        assertEquals("metricName's isCompleteName value", true, resultList.get(0).isCompleteName());
    }

    @Test
    public void testAddingMultipleCompleteMetricName() {
        MetricNameListBuilder tokenListBuilder = new MetricNameListBuilder();
        Set<String> metricNames = new HashSet<String>() {{
            add("foo");
            add("bar");
        }};
        tokenListBuilder.addCompleteMetricName(metricNames);

        List<MetricName> resultList = tokenListBuilder.build();
        assertEquals("result size", 2, resultList.size());

        Set<String> outputMetricNames = new HashSet<String>();
        for (MetricName metricName : resultList) {
            outputMetricNames.add(metricName.getName());
            assertEquals("token's isCompleteName value", true, metricName.isCompleteName());
        }

        Assert.assertTrue("outputMetricNames should not have more than expected", metricNames.containsAll(outputMetricNames));
        Assert.assertTrue("all outputMetricNames should be in the expected", outputMetricNames.containsAll(metricNames));
    }
}
