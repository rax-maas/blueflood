/*
 * Copyright 2013-2016 Rackspace
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

package com.rackspacecloud.blueflood.outputs.handlers;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rackspacecloud.blueflood.http.HttpIntegrationTestBase;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * Integration Tests for POST .../views (aka Multiplot views)
 */
public class HttpMultiRollupsQueryHandlerIntegrationTest extends HttpIntegrationTestBase {

    private final long TIME_DIFF = 2000;
    private final String tenant_id = "333333";
    private long start = System.currentTimeMillis() - TIME_DIFF;
    private long end = System.currentTimeMillis() + TIME_DIFF;

    @Test
    public void testHttpMultiRollupsQueryHandler() throws Exception {
        // ingest and rollup metrics with enum values and verify CF points and elastic search indexes
        String postfix = getPostfix();

        // post multi metrics for ingestion and verify
        HttpResponse response = postMetric(tenant_id, postAggregatedPath, "sample_payload.json", postfix);
        assertEquals( "Should get status 200 from ingestion server for POST", 200, response.getStatusLine().getStatusCode() );
        EntityUtils.consume(response.getEntity());

        JsonObject responseObject = getMultiMetricRetry( tenant_id, start, end, "200", "FULL", "",
                "['3333333.G1s" + postfix + "','3333333.G10s" + postfix + "']", 2 );

        assertNotNull( "No values for metrics found", responseObject );

        JsonArray metrics = responseObject.getAsJsonArray( "metrics" );
        assertEquals( 2, metrics.size() );

        Map<String, JsonObject> metricMap = new HashMap<String, JsonObject>();
        JsonObject metric0 = metrics.get( 0 ).getAsJsonObject();
        metricMap.put( metric0.get( "metric" ).getAsString(), metric0 );

        JsonObject metric1 = metrics.get( 1 ).getAsJsonObject();
        metricMap.put(  metric1.get( "metric" ).getAsString(), metric1 );

        JsonObject metricCheck1 = metricMap.get( "3333333.G1s" + postfix );
        assertNotNull( metricCheck1 );

        assertEquals( "unknown", metricCheck1.get( "unit" ).getAsString() );
        assertEquals( "number", metricCheck1.get( "type" ).getAsString() );
        JsonArray data0 = metricCheck1.getAsJsonArray( "data" );
        assertEquals( 1, data0.size() );

        JsonObject data0a = data0.get( 0 ).getAsJsonObject();
        assertTrue( data0a.has( "timestamp" ) );
        assertEquals( 1, data0a.get( "numPoints" ).getAsInt() );
        assertEquals( 397, data0a.get( "latest" ).getAsInt() );


        JsonObject metricCheck2 = metricMap.get( "3333333.G10s" + postfix );
        assertNotNull( metricCheck2 );

        assertEquals( "unknown", metricCheck2.get( "unit" ).getAsString() );
        assertEquals( "number", metricCheck2.get( "type" ).getAsString() );

        JsonArray data1 = metricCheck2.getAsJsonArray( "data" );
        assertEquals( 1, data1.size() );

        JsonObject data1a = data1.get( 0 ).getAsJsonObject();
        assertTrue( data1a.has( "timestamp" ) );
        assertEquals(1, data1a.get("numPoints").getAsInt());
        assertEquals(56, data1a.get("latest").getAsInt());

        assertResponseHeaderAllowOrigin(response);
    }

    @Test
    @Ignore
    public void testHttpMultiRollupsQueryHandler_WithEnum() throws Exception {

        String postfix = getPostfix();

        // ingest and rollup metrics with enum values and verify CF points and elastic search indexes
        final String tenant_id_enum = "99988877";
        final String metric_name = "call_xyz_api" + postfix;

        // post multi metrics for ingestion and verify
        HttpResponse response = postMetric(tenant_id_enum, postAggregatedMultiPath, "sample_multi_enums_payload.json", postfix);
        assertEquals( "Should get status 200 from ingestion server for POST", 200, response.getStatusLine().getStatusCode() );
        EntityUtils.consume(response.getEntity());

        JsonObject responseObject = getMultiMetricRetry( tenant_id_enum, start, end, "", "FULL", "enum_values", String.format("['%s']", metric_name), 1 );

        assertNotNull( "No values for metrics found", responseObject );

        JsonArray metrics = responseObject.getAsJsonArray( "metrics" );
        assertEquals( 1, metrics.size() );

        JsonObject metric = metrics.get( 0 ).getAsJsonObject();
        assertEquals( "unknown", metric.get( "unit" ).getAsString() );
        assertEquals( metric_name, metric.get( "metric" ).getAsString() );
        assertEquals( "enum", metric.get( "type" ).getAsString() );

        JsonArray data = metric.getAsJsonArray( "data" );
        assertEquals( 2, data.size() );

        JsonObject data1 = data.get( 0 ).getAsJsonObject();
        assertTrue( data1.has( "timestamp" ) );
        assertEquals( 1, data1.getAsJsonObject( "enum_values" ).get( "OK" ).getAsInt() );

        assertResponseHeaderAllowOrigin(response);
    }

    /**
     *
     * On Travis we had runs fails because even though the query returns 200, no metric values are on in the response.
     *
     * We aren't sure what's going on as we can't reproduce this locally.
     *
     * @param tenant_id
     * @param start
     * @param end
     * @param points
     * @param resolution
     * @param select
     * @param metricNames
     * @param size
     * @return
     * @throws InterruptedException
     * @throws IOException
     * @throws URISyntaxException
     */
    private JsonObject getMultiMetricRetry( String tenant_id,
                                            long start,
                                            long end,
                                            String points,
                                            String resolution,
                                            String select,
                                            String metricNames,
                                            int size ) throws InterruptedException, IOException, URISyntaxException {

        for( int i = 0; i < 10 ; i++ ) {

            // query for multiplot metric and assert results
            HttpResponse query_response = queryMultiplot( tenant_id, start, end, points, resolution, select,
                    metricNames );
            assertEquals( "Should get status 200 from query server for multiplot POST", 200, query_response.getStatusLine().getStatusCode() );

            // assert response content
            String responseContent = EntityUtils.toString( query_response.getEntity(), "UTF-8" );

            JsonParser jsonParser = new JsonParser();
            JsonObject responseObject = jsonParser.parse( responseContent ).getAsJsonObject();

            if( responseObject.getAsJsonArray( "metrics" ).size() == size )
                return responseObject;

            Thread.currentThread().sleep( 5000 );
        }

        return null;
    }
}
