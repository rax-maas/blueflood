/*
 * Copyright 2013 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.inputs.formats;

import com.rackspacecloud.blueflood.types.Metric;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class JSONMetricsContainerTest {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final StringWriter writer = new StringWriter();
    private final TypeFactory typeFactory = TypeFactory.defaultInstance();

    @Test
    public void testJSONMetricsContainerConstruction() throws Exception {
        // This part tests Jackson JSON mapper
        List<JSONMetricsContainer.JSONMetric> jsonMetrics =
                mapper.readValue(
                        generateJSONMetricsData(),
                        typeFactory.constructCollectionType(List.class,
                                JSONMetricsContainer.JSONMetric.class)
                );
        // Construct the JSONMetricsContainter from JSON metric objects
        JSONMetricsContainer jsonMetricsContainer = new JSONMetricsContainer("ac1", jsonMetrics);

        List<Metric> metricsCollection = jsonMetricsContainer.toMetrics();
        Assert.assertEquals("ac1.mzord.duration", metricsCollection.get(0).getLocator().toString());
        Assert.assertEquals(Long.MAX_VALUE, metricsCollection.get(0).getValue());
        Assert.assertEquals(1234566, metricsCollection.get(0).getTtlInSeconds());
        Assert.assertEquals(1234567890L, metricsCollection.get(0).getCollectionTime());
        Assert.assertEquals("milliseconds", metricsCollection.get(0).getUnit());
        Assert.assertEquals("L", metricsCollection.get(0).getType().toString());

        Assert.assertEquals("ac1.mzord.status", metricsCollection.get(1).getLocator().toString());
        Assert.assertEquals("Website is up", metricsCollection.get(1).getValue());
        Assert.assertEquals("unknown", metricsCollection.get(1).getUnit());
        Assert.assertEquals("S", metricsCollection.get(1).getType().toString());
    }

    public static String generateJSONMetricsData() throws Exception {
        List<Object> metricsList = new ArrayList<Object>();

        // Long metric value
        Map<String, Object> testMetric = new TreeMap<String, Object>();
        testMetric.put("metricName", "mzord.duration");
        testMetric.put("ttlInSeconds", 1234566);
        testMetric.put("unit", "milliseconds");
        testMetric.put("metricValue", Long.MAX_VALUE);
        testMetric.put("collectionTime", 1234567890L);
        metricsList.add(testMetric);

        // String metric value
        testMetric = new TreeMap<String, Object>();
        testMetric.put("metricName", "mzord.status");
        testMetric.put("ttlInSeconds", 1234566);
        testMetric.put("unit", "unknown");
        testMetric.put("metricValue", "Website is up");
        testMetric.put("collectionTime", 1234567890L);
        metricsList.add(testMetric);

        mapper.writeValue(writer, metricsList);
        final String jsonString = writer.toString();

        return jsonString;
    }
}
