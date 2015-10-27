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

import com.rackspacecloud.blueflood.http.HttpIntegrationTestBase;
import com.rackspacecloud.blueflood.types.Locator;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class HttpMultiRollupsQueryHandlerIntegrationTest extends HttpIntegrationTestBase {
    private final long fromTime = 1389124830L;
    private final long toTime = 1389211230L;

    @Test
    public void testMultiplotQuery() throws Exception {
        // ingest and rollup metrics with enum values and verify CF points and elastic search indexes
        final String tenant_id = "333333";

        // post multi metrics for ingestion and verify
        HttpResponse response = postAggregatedMetric(tenant_id, "src/test/resources/sample_payload.json");
        Assert.assertEquals("Should get status 200 from ingestion server for POST", 200, response.getStatusLine().getStatusCode());
        EntityUtils.consume(response.getEntity());

        // query for multiplot metric and assert results
        HttpResponse query_response = queryMultiplot(tenant_id, fromTime, toTime, "", "FULL", "enum_values", "['3333333.G1s','3333333.G10s']");
        Assert.assertEquals("Should get status 200 from query server for multiplot POST", 200, query_response.getStatusLine().getStatusCode());

        // assert response content
        String responseContent = EntityUtils.toString(query_response.getEntity(), "UTF-8");
        String expectedResponse = "{\n" +
                "  \"metrics\": [\n" +
                "    {\n" +
                "      \"unit\": \"unknown\",\n" +
                "      \"metric\": \"3333333.G1s\",\n" +
                "      \"data\": [\n" +
                "        {\n" +
                "          \"timestamp\": 1389211230\n" +
                "        }\n" +
                "      ],\n" +
                "      \"type\": \"number\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"unit\": \"unknown\",\n" +
                "      \"metric\": \"3333333.G10s\",\n" +
                "      \"data\": [\n" +
                "        {\n" +
                "          \"timestamp\": 1389211230\n" +
                "        }\n" +
                "      ],\n" +
                "      \"type\": \"number\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        Assert.assertEquals(expectedResponse, responseContent);
        EntityUtils.consume(query_response.getEntity());
    }

    @Test
    public void testMultiplotQueryWithEnum() throws Exception {
        // ingest and rollup metrics with enum values and verify CF points and elastic search indexes
        final String tenant_id = "333333";

        // post multi metrics for ingestion and verify
        HttpResponse response = postAggregatedMetric(tenant_id, "src/test/resources/sample_enums_payload.json");
        Assert.assertEquals("Should get status 200 from ingestion server for POST", 200, response.getStatusLine().getStatusCode());
        EntityUtils.consume(response.getEntity());

        // query for multiplot metric and assert results
        HttpResponse query_response = queryMultiplot(tenant_id, fromTime, toTime, "", "FULL", "enum_values", "['enum_metric_test']");
        Assert.assertEquals("Should get status 200 from query server for multiplot POST", 200, query_response.getStatusLine().getStatusCode());

        // assert response content
        String responseContent = EntityUtils.toString(query_response.getEntity(), "UTF-8");
        String expectedResponse = "{\n" +
                "  \"metrics\": [\n" +
                "    {\n" +
                "      \"unit\": \"unknown\",\n" +
                "      \"metric\": \"enum_metric_test\",\n" +
                "      \"data\": [\n" +
                "        {\n" +
                "          \"timestamp\": 1389211230,\n" +
                "          \"enum_values\": {\n" +
                "            \"v3\": 1\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"type\": \"number\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        Assert.assertEquals(expectedResponse, responseContent);
        EntityUtils.consume(query_response.getEntity());
    }
}
