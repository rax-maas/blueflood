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
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;

public class HttpRollupsQueryHandlerIntegrationTest extends HttpIntegrationTestBase {
    private final long fromTime = 1389124830L;
    private final long toTime = 1389211230L;

    @Test
    public void testSingleplotQuery() throws Exception {
        // ingest and rollup metrics with enum values and verify CF points and elastic search indexes
        final String tenant_id = "333333";
        final String metric_name = "3333333.G1s";

        // post multi metrics for ingestion and verify
        HttpResponse response = postMetric(tenant_id, postAggregatedPath, "src/test/resources/sample_payload.json");
        Assert.assertEquals("Should get status 200 from ingestion server for POST", 200, response.getStatusLine().getStatusCode());
        EntityUtils.consume(response.getEntity());

        // query for multiplot metric and assert results
        HttpResponse query_response = querySingleplot(tenant_id, metric_name, fromTime, toTime, "", "FULL", "");
        Assert.assertEquals("Should get status 200 from query server for single view GET", 200, query_response.getStatusLine().getStatusCode());

        // assert response content
        String responseContent = EntityUtils.toString(query_response.getEntity(), "UTF-8");
        String expectedResponse = "{\n" +
                "  \"unit\": \"unknown\",\n" +
                "  \"values\": [\n" +
                "    {\n" +
                "      \"numPoints\": 1,\n" +
                "      \"timestamp\": 1389211230,\n" +
                "      \"latest\": 397\n" +
                "    }\n" +
                "  ],\n" +
                "  \"metadata\": {\n" +
                "    \"limit\": null,\n" +
                "    \"next_href\": null,\n" +
                "    \"count\": 1,\n" +
                "    \"marker\": null\n" +
                "  }\n" +
                "}";

        Assert.assertEquals(expectedResponse, responseContent);
        EntityUtils.consume(query_response.getEntity());
    }

    @Test
    public void testSingleplotQueryWithEnum() throws Exception {
        // ingest and rollup metrics with enum values and verify CF points and elastic search indexes
        final String tenant_id = "333333";
        final String metric_name = "enum_metric_test";

        // post multi metrics for ingestion and verify
        HttpResponse response = postMetric(tenant_id, postAggregatedPath, "src/test/resources/sample_enums_payload.json");
        Assert.assertEquals("Should get status 200 from ingestion server for POST", 200, response.getStatusLine().getStatusCode());
        EntityUtils.consume(response.getEntity());

        // query for multiplot metric and assert results
        HttpResponse query_response = querySingleplot(tenant_id, metric_name, fromTime, toTime, "", "FULL", "");
        Assert.assertEquals("Should get status 200 from query server for single view GET", 200, query_response.getStatusLine().getStatusCode());

        // assert response content
        String responseContent = EntityUtils.toString(query_response.getEntity(), "UTF-8");
        String expectedResponse = "{\n" +
                "  \"unit\": \"unknown\",\n" +
                "  \"values\": [\n" +
                "    {\n" +
                "      \"numPoints\": 1,\n" +
                "      \"timestamp\": 1389211230,\n" +
                "      \"enum_values\": {\n" +
                "        \"v3\": 1\n" +
                "      },\n" +
                "      \"type\": \"enum\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"metadata\": {\n" +
                "    \"limit\": null,\n" +
                "    \"next_href\": null,\n" +
                "    \"count\": 1,\n" +
                "    \"marker\": null\n" +
                "  }\n" +
                "}";

        Assert.assertEquals(expectedResponse, responseContent);
        EntityUtils.consume(query_response.getEntity());
    }
}
