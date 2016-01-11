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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rackspacecloud.blueflood.http.HttpIntegrationTestBase;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;

public class HttpMultiRollupsQueryHandlerIntegrationTest extends HttpIntegrationTestBase {
    private final long fromTime = 1389124830L;
    private final long toTime = 1439231325000L;

    @Test
    public void testMultiplotQuery() throws Exception {
        // ingest and rollup metrics with enum values and verify CF points and elastic search indexes
        final String tenant_id = "333333";

        // post multi metrics for ingestion and verify
        HttpResponse response = postMetric(tenant_id, postAggregatedPath, "src/test/resources/sample_payload.json");
        Assert.assertEquals("Should get status 200 from ingestion server for POST", 200, response.getStatusLine().getStatusCode());
        EntityUtils.consume(response.getEntity());

        // query for multiplot metric and assert results
        HttpResponse query_response = queryMultiplot(tenant_id, fromTime, toTime, "200", "FULL", "", "['3333333.G1s','3333333.G10s']");
        Assert.assertEquals("Should get status 200 from query server for multiplot POST", 200, query_response.getStatusLine().getStatusCode());

        // assert response content
        String responseContent = EntityUtils.toString(query_response.getEntity(), "UTF-8");

        JsonParser jsonParser = new JsonParser();
        JsonObject responseObject = jsonParser.parse(responseContent).getAsJsonObject();

        String expectedResponse = "{\n" +
                "  \"metrics\": [\n" +
                "    {\n" +
                "      \"unit\": \"unknown\",\n" +
                "      \"metric\": \"3333333.G1s\",\n" +
                "      \"data\": [\n" +
                "        {\n" +
                "          \"numPoints\": 1,\n" +
                "          \"timestamp\": 1382400000,\n" +
                "          \"latest\": 397\n" +
                "        }\n" +
                "      ],\n" +
                "      \"type\": \"number\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"unit\": \"unknown\",\n" +
                "      \"metric\": \"3333333.G10s\",\n" +
                "      \"data\": [\n" +
                "        {\n" +
                "          \"numPoints\": 1,\n" +
                "          \"timestamp\": 1382400000,\n" +
                "          \"latest\": 56\n" +
                "        }\n" +
                "      ],\n" +
                "      \"type\": \"number\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";
        JsonObject expectedObject = jsonParser.parse(expectedResponse).getAsJsonObject();

        Assert.assertEquals(expectedObject, responseObject);
        EntityUtils.consume(query_response.getEntity());
    }

    @Test
    public void testMultiplotQueryWithEnum() throws Exception {
        // ingest and rollup metrics with enum values and verify CF points and elastic search indexes
        final String tenant_id = "99988877";
        final String metric_name = "call_xyz_api";

        // post multi metrics for ingestion and verify
        HttpResponse response = postMetric(tenant_id, postAggregatedMultiPath, "src/test/resources/sample_multi_enums_payload.json");
        Assert.assertEquals("Should get status 200 from ingestion server for POST", 200, response.getStatusLine().getStatusCode());
        EntityUtils.consume(response.getEntity());

        // query for multiplot metric and assert results
        HttpResponse query_response = queryMultiplot(tenant_id, fromTime, toTime, "", "FULL", "enum_values", String.format("['%s']", metric_name));
        Assert.assertEquals("Should get status 200 from query server for multiplot POST", 200, query_response.getStatusLine().getStatusCode());

        // assert response content
        String responseContent = EntityUtils.toString(query_response.getEntity(), "UTF-8");

        JsonParser jsonParser = new JsonParser();
        JsonObject responseObject = jsonParser.parse(responseContent).getAsJsonObject();

        String expectedResponse = String.format("{\n" +
                "  \"metrics\": [\n" +
                "    {\n" +
                "      \"unit\": \"unknown\",\n" +
                "      \"metric\": \"%s\",\n" +
                "      \"data\": [\n" +
                "        {\n" +
                "          \"timestamp\": 1439231324001,\n" +
                "          \"enum_values\": {\n" +
                "            \"OK\": 1\n" +
                "          }\n" +
                "        },\n" +
                "        {\n" +
                "          \"timestamp\": 1439231324003,\n" +
                "          \"enum_values\": {\n" +
                "            \"ERROR\": 1\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"type\": \"enum\"\n" +
                "    }\n" +
                "  ]\n" +
                "}", metric_name);

        JsonObject expectedObject = jsonParser.parse(expectedResponse).getAsJsonObject();

        Assert.assertEquals(expectedObject, responseObject);
        EntityUtils.consume(query_response.getEntity());
    }
}
