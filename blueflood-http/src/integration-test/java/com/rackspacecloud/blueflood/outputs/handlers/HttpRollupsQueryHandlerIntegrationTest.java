/*
 * Copyright 2013-2015 Rackspace
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
import org.junit.Test;

import static org.junit.Assert.*;

public class HttpRollupsQueryHandlerIntegrationTest extends HttpIntegrationTestBase {

    @Test
    public void testSingleplotQuery() throws Exception {

        long start = System.currentTimeMillis() - TIME_DIFF;
        long end = System.currentTimeMillis() + TIME_DIFF;

        String prefix = getPrefix();

        // ingest and rollup metrics with enum values and verify CF points and elastic search indexes
        final String tenant_id = "333333";
        final String metric_name = prefix + "3333333.G1s";

        // post multi metrics for ingestion and verify
        HttpResponse response = postMetric(tenant_id, postAggregatedPath, "sample_payload.json", prefix );
        assertEquals( "Should get status 200 from ingestion server for POST", 200, response.getStatusLine().getStatusCode() );
        EntityUtils.consume(response.getEntity());

        // query for multiplot metric and assert results
        HttpResponse query_response = querySingleplot(tenant_id, metric_name, start, end, "", "FULL", "");
        assertEquals( "Should get status 200 from query server for single view GET", 200, query_response.getStatusLine().getStatusCode() );

        // assert response content
        String responseContent = EntityUtils.toString(query_response.getEntity(), "UTF-8");

        JsonParser jsonParser = new JsonParser();
        JsonObject responseObject = jsonParser.parse(responseContent).getAsJsonObject();

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
    }

    @Test
    public void testSingleplotQueryWithEnum() throws Exception {

        long start = System.currentTimeMillis() - TIME_DIFF;
        long end = System.currentTimeMillis() + TIME_DIFF;

        String prefix = getPrefix();

        // ingest and rollup metrics with enum values and verify CF points and elastic search indexes
        final String tenant_id = "333333";
        final String metric_name = prefix + "enum_metric_test";

        // post multi metrics for ingestion and verify
        HttpResponse response = postMetric(tenant_id, postAggregatedPath, "sample_enums_payload.json", prefix );
        assertEquals( "Should get status 200 from ingestion server for POST", 200, response.getStatusLine().getStatusCode() );
        EntityUtils.consume(response.getEntity());

        // query for multiplot metric and assert results
        HttpResponse query_response = querySingleplot(tenant_id, metric_name, start, end, "", "FULL", "");
        assertEquals( "Should get status 200 from query server for single view GET", 200, query_response.getStatusLine().getStatusCode() );

        // assert response content
        String responseContent = EntityUtils.toString(query_response.getEntity(), "UTF-8");

        JsonParser jsonParser = new JsonParser();
        JsonObject responseObject = jsonParser.parse(responseContent).getAsJsonObject();

        assertEquals( "unknown", responseObject.get( "unit" ).getAsString() );

        JsonArray values = responseObject.getAsJsonArray( "values" );
        assertEquals( 1, values.size() );

        JsonObject value = values.get( 0 ).getAsJsonObject();

        assertEquals( 1, value.get( "numPoints" ).getAsInt() );
        assertTrue( value.has( "timestamp" ) );
        assertEquals( 1, value.getAsJsonObject( "enum_values" ).get( "v3" ).getAsInt() );
        assertEquals( "enum", value.get( "type").getAsString() );

        JsonObject meta = responseObject.getAsJsonObject( "metadata" );
        assertTrue( meta.get( "limit" ).isJsonNull() );
        assertTrue( meta.get( "next_href" ).isJsonNull() );
        assertEquals( 1,  meta.get( "count" ).getAsInt() );
        assertTrue( meta.get( "marker" ).isJsonNull() );

    }
}
