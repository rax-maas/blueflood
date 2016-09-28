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

import com.rackspacecloud.blueflood.inputs.handlers.HttpMetricsIngestionHandlerTest;
import com.rackspacecloud.blueflood.outputs.formats.ErrorResponse;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.Metric;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.rackspacecloud.blueflood.TestUtils.generateJSONMetricsData;
import static org.junit.Assert.*;

public class JSONMetricsContainerTest {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final long MINUTE = 60000;

    private final TypeFactory typeFactory = TypeFactory.defaultInstance();
    private final long current = System.currentTimeMillis();
    @Test
    public void testJSONMetricsContainerConstruction() throws Exception {
         // Construct the JSONMetricsContainter from JSON metric objects
        JSONMetricsContainer jsonMetricsContainer = getContainer( "ac1", generateJSONMetricsData() );

        List<Metric> metricsCollection = jsonMetricsContainer.getValidMetrics();

        assertTrue( jsonMetricsContainer.getValidationErrors().isEmpty() );
        assertTrue( metricsCollection.size() == 2 );
        assertEquals( "ac1.mzord.duration", metricsCollection.get( 0 ).getLocator().toString() );
        assertEquals( Long.MAX_VALUE, metricsCollection.get( 0 ).getMetricValue() );
        assertEquals( 1234566, metricsCollection.get( 0 ).getTtlInSeconds() );
        assertTrue( current - metricsCollection.get( 0 ).getCollectionTime() < MINUTE );
        assertEquals( "milliseconds", metricsCollection.get( 0 ).getUnit() );
        assertEquals( "N", metricsCollection.get( 0 ).getDataType().toString() );

        assertEquals( "ac1.mzord.status", metricsCollection.get( 1 ).getLocator().toString() );
        assertEquals( "Website is up", metricsCollection.get( 1 ).getMetricValue() );
        assertEquals( "unknown", metricsCollection.get( 1 ).getUnit() );
        assertEquals( "S", metricsCollection.get( 1 ).getDataType().toString() );
    }

    @Test
    public void testBigIntHandling() throws IOException {
        String jsonBody = "[{\"collectionTime\": " + current + ",\"ttlInSeconds\":172800,\"metricValue\":18446744073709000000,\"metricName\":\"used\",\"unit\":\"unknown\"}]";

        JSONMetricsContainer container = getContainer("786659", jsonBody);

        List<Metric> metrics = container.getValidMetrics();
        assertTrue(container.getValidationErrors().isEmpty());
    }

    @Test
    public void testDelayedMetric() throws Exception {
        long time = current - 1000 - Configuration.getInstance().getLongProperty(CoreConfig.TRACKER_DELAYED_METRICS_MILLIS);
        String jsonBody = "[{\"collectionTime\": " + time  + ",\"ttlInSeconds\":172800,\"metricValue\":1844,\"metricName\":\"metricName1\",\"unit\":\"unknown\"}]";

        JSONMetricsContainer container = getContainer("786659", jsonBody );

        // has a side-effect required by areDelayedMetricsPresent()
        List<Metric> metrics = container.getValidMetrics();

        assertTrue( container.getValidationErrors().isEmpty() );
        assertTrue(container.areDelayedMetricsPresent());
    }

    @Test
    public void testDelayedMetricFalseForRecentMetric() throws Exception {
        String jsonBody = "[{\"collectionTime\":" + current + ",\"ttlInSeconds\":172800,\"metricValue\":1844,\"metricName\":\"metricName1\",\"unit\":\"unknown\"}]";

        JSONMetricsContainer container = getContainer( "786659", jsonBody );

        // has a side-effect required by areDelayedMetricsPresent()
        List<Metric> metrics = container.getValidMetrics();

        assertTrue( container.getValidationErrors().isEmpty() );
        assertFalse(container.areDelayedMetricsPresent());
    }

    @Test
    public void testScopedJsonMetric() throws IOException {
        String jsonBody = "[{\"tenantId\": 12345, \"collectionTime\":" + current + ",\"ttlInSeconds\":172800,\"metricValue\":1844,\"metricName\":\"metricName1\",\"unit\":\"unknown\"}]";

        JSONMetricsContainer container =  getScopedContainer("786659", jsonBody);
        assertTrue( container.getValidationErrors().isEmpty() );
    }



    private JSONMetricsContainer getScopedContainer( String name, String jsonBody ) throws java.io.IOException {

        List<JSONMetric> jsonMetrics =
                mapper.readValue(
                        jsonBody,
                        typeFactory.constructCollectionType(List.class, JSONMetricScoped.class)
                );
        return new JSONMetricsContainer( name, jsonMetrics, new ArrayList<ErrorResponse.ErrorData>());
    }
    private JSONMetricsContainer getContainer( String tenantId, String jsonBody ) throws java.io.IOException {

        return new HttpMetricsIngestionHandlerTest().getContainer(tenantId, jsonBody);
    }

}
