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
import com.rackspacecloud.blueflood.io.EventElasticSearchIO;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Integration Tests for GET .../views/:metricName (aka Singleplot views)
 */
public class HttpRollupsQueryHandlerIntegrationTest extends HttpIntegrationTestBase {

    private final String tenant_id = "333333";
    private final long now = System.currentTimeMillis();
    private final long start = now - TIME_DIFF_MS;
    private final long end = now + TIME_DIFF_MS;

    @Before
    public void setup() throws Exception {
        super.esSetup();
        ((EventElasticSearchIO) eventsSearchIO).setClient(getClient());
    }

    @Test
    public void testHttpRollupsQueryHandler() throws Exception {

        String postfix = getPostfix();

        // ingest and rollup metrics with enum values and verify CF points and elastic search indexes
        final String metric_name = "3333333.G1s" + postfix;

        // post multi metrics for ingestion and verify
        HttpResponse response = postMetric(tenant_id, postAggregatedPath, "sample_payload.json", postfix );
        assertEquals( "Should get status 200 from ingestion server for POST", 200, response.getStatusLine().getStatusCode() );
        EntityUtils.consume(response.getEntity());

        JsonObject responseObject = getSingleMetricRetry( tenant_id, metric_name, start, end, "", "FULL", "");

        assertNotNull( "No values for " + metric_name + " found", responseObject );

        assertEquals( "unknown", responseObject.get( "unit" ).getAsString() );

        JsonArray values = responseObject.getAsJsonArray( "values" );
        assertEquals( 1, values.size() );

        JsonObject value = values.get( 0 ).getAsJsonObject();
        assertEquals( 1, value.get( "numPoints" ).getAsInt() );
        assertTrue( value.has( "timestamp" ) );
        assertEquals( 397, value.get( "latest" ).getAsInt() );

        JsonObject meta = responseObject.get( "metadata" ).getAsJsonObject();
        assertTrue( meta.get( "limit" ).isJsonNull() );
        assertTrue( meta.get( "next_href" ).isJsonNull() );
        assertEquals( 1,  meta.get( "count" ).getAsInt() );
        assertTrue( meta.get( "marker" ).isJsonNull() );

        assertResponseHeaderAllowOrigin(response);
    }

    /**
     *
     * On Travis we had runs fails because even though the query returns 200, no metric values are on in the response.
     *
     * We aren't sure what's going on as we can't reproduce this locally.
     *
     * @param tenant_id
     * @param metric_name
     * @param start
     * @param end
     * @param points
     * @param resolution
     * @param select
     * @return
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    private JsonObject getSingleMetricRetry(String tenant_id,
                                            String metric_name,
                                            long start,
                                            long end,
                                            String points,
                                            String resolution,
                                            String select) throws URISyntaxException, IOException, InterruptedException {

        for( int i = 0; i < 10 ; i++ ) {
            // query for multiplot metric and assert results
            HttpResponse query_response = querySingleplot( tenant_id, metric_name, start, end, points, resolution, select );
            assertEquals( "Should get status 200 from query server for single view GET", 200, query_response.getStatusLine().getStatusCode() );

            // assert response content
            String responseContent = EntityUtils.toString( query_response.getEntity(), "UTF-8" );

            JsonParser jsonParser = new JsonParser();
            JsonObject responseObject = jsonParser.parse( responseContent ).getAsJsonObject();

            JsonArray arrayValues  = responseObject.getAsJsonArray( "values" );
            if ( arrayValues.size() == 1 ) {
                return responseObject;
            }

            System.out.println(String.format("Data for metric %s is not found, sleeping and retrying, payload: %s", metric_name, responseContent));
            Thread.currentThread().sleep( 5000 );

        }
        return null;
    }
}
