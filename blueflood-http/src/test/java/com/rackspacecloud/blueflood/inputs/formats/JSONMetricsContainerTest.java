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
import java.util.*;

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

        Assert.assertTrue(metricsCollection.size() == 2);
        Assert.assertEquals("ac1.mzord.duration", metricsCollection.get(0).getLocator().toString());
        Assert.assertEquals(Long.MAX_VALUE, metricsCollection.get(0).getMetricValue());
        Assert.assertEquals(1234566, metricsCollection.get(0).getTtlInSeconds());
        Assert.assertEquals(1234567890L, metricsCollection.get(0).getCollectionTime());
        Assert.assertEquals("milliseconds", metricsCollection.get(0).getUnit());
        Assert.assertEquals("N", metricsCollection.get(0).getDataType().toString());

        Assert.assertEquals("ac1.mzord.status", metricsCollection.get(1).getLocator().toString());
        Assert.assertEquals("Website is up", metricsCollection.get(1).getMetricValue());
        Assert.assertEquals("unknown", metricsCollection.get(1).getUnit());
        Assert.assertEquals("S", metricsCollection.get(1).getDataType().toString());
    }

    @Test
    public void testBigIntHandling() {
        String jsonBody = "[{\"collectionTime\":1401302372775,\"ttlInSeconds\":172800,\"metricValue\":18446744073709000000,\"metricName\":\"used\",\"unit\":\"unknown\"}]";

        JSONMetricsContainer container = null;
        try {
            List<JSONMetricsContainer.JSONMetric> jsonMetrics =
                mapper.readValue(
                        jsonBody,
                        typeFactory.constructCollectionType(List.class,
                                JSONMetricsContainer.JSONMetric.class)
                );
            container = new JSONMetricsContainer("786659", jsonMetrics);
        } catch (Exception e) {
            Assert.fail("Jackson failed to parse a big int");
        }

        try {
            List<Metric> metrics = container.toMetrics();
        } catch (Exception ex) {
            Assert.fail();
        }
    }

    public static List<Map<String, Object>> generateMetricsData() throws Exception {
        List<Map<String, Object>> metricsList = new ArrayList<Map<String, Object>>();

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

        // null metric value. This shouldn't be in the final list of metrics because we ignore null valued metrics.
        testMetric = new TreeMap<String, Object>();
        testMetric.put("metricName", "mzord.hipster");
        testMetric.put("ttlInSeconds", 1234566);
        testMetric.put("unit", "unknown");
        testMetric.put("metricValue", null);
        testMetric.put("collectionTime", 1234567890L);
        metricsList.add(testMetric);
        return metricsList;
    }

    public static List<Map<String, Object>> generateAnnotationsData() throws Exception {
        List<Map<String, Object>> annotationsList = new ArrayList<Map<String, Object>>();

        // Long metric value
        Map<String, Object> testAnnotation = new TreeMap<String, Object>();
        testAnnotation.put("what","deployment");
        testAnnotation.put("when", Calendar.getInstance().getTimeInMillis());
        testAnnotation.put("tags","prod");
        testAnnotation.put("data","app00.restart");

        annotationsList.add(testAnnotation);
        return annotationsList;
    }

    public static String generateJSONAnnotationsData() throws Exception {
        mapper.writeValue(writer, generateAnnotationsData());
        final String jsonString = writer.toString();

        return jsonString;
    }

    public static String generateJSONMetricsData() throws Exception {
        mapper.writeValue(writer, generateMetricsData());
        final String jsonString = writer.toString();

        return jsonString;
    }

    public static String generateMultitenantJSONMetricsData() throws Exception {
        List<Map<String, Object>> dataOut = new ArrayList<Map<String, Object>>();
        for (Map<String, Object> stringObjectMap : generateMetricsData()) {
            stringObjectMap.put("tenantId", "tenantOne");
            dataOut.add(stringObjectMap);
        }
        for (Map<String, Object> stringObjectMap : generateMetricsData()) {
            stringObjectMap.put("tenantId", "tenantTwo");
            dataOut.add(stringObjectMap);
        }

        return mapper.writeValueAsString(dataOut);
    }
}
