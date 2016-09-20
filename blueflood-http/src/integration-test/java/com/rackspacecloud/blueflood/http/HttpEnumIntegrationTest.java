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

package com.rackspacecloud.blueflood.http;

import com.rackspacecloud.blueflood.outputs.formats.ErrorResponse;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.service.EnumValidator;
import com.rackspacecloud.blueflood.types.Locator;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.*;
import static com.rackspacecloud.blueflood.TestUtils.*;

public class HttpEnumIntegrationTest extends HttpIntegrationTestBase {

    @Test
    public void testMetricIngestionWithEnum() throws Exception {

        // ingest and rollup metrics with enum values and verify CF points and elastic search indexes
        final String tenant_id = "333333";

        String postfix = getPostfix();
        final String metric_name = "enum_metric_test" + postfix;

        Set<Locator> locators = new HashSet<Locator>();
        locators.add(Locator.createLocatorFromPathComponents(tenant_id, metric_name));

        // post enum metric for ingestion and verify
        HttpResponse response = postMetric(tenant_id, postAggregatedPath, "sample_enums_payload.json", postfix );
        assertEquals( "Should get status 200 from ingestion server for POST", 200, response.getStatusLine().getStatusCode() );
        EntityUtils.consume(response.getEntity());

        // execute EnumValidator
        EnumValidator enumValidator = new EnumValidator(locators);
        enumValidator.run();

        //Sleep for a while
        Thread.sleep(3000);

        // query for metric and assert results
        HttpResponse query_response = queryMetricIncludeEnum(tenant_id, metric_name);
        assertEquals( "Should get status 200 from query server for GET", 200, query_response.getStatusLine().getStatusCode() );

        // assert response content
        String responseContent = EntityUtils.toString(query_response.getEntity(), "UTF-8");
        assertEquals( String.format( "[{\"metric\":\"%s\",\"enum_values\":[\"v1\",\"v2\",\"v3\"]}]", metric_name ), responseContent );
        EntityUtils.consume(query_response.getEntity());
    }

    @Test
    public void testHttpEnumIngestionInvalidPastCollectionTime() throws IOException, URISyntaxException {

        long timestamp = System.currentTimeMillis() - TIME_DIFF_MS - Configuration.getInstance().getLongProperty( CoreConfig.BEFORE_CURRENT_COLLECTIONTIME_MS );

        // ingest and rollup metrics with enum values and verify CF points and elastic search indexes
        final String tenant_id = "333333";

        String postfix = getPostfix();

        final String metric_name = postfix + "enum_metric_test";
        Set<Locator> locators = new HashSet<Locator>();
        locators.add(Locator.createLocatorFromPathComponents(tenant_id, metric_name));

        // post enum metric for ingestion and verify
        HttpResponse response = postMetric(tenant_id, postAggregatedPath, "sample_enums_payload.json", timestamp, postfix );
        ErrorResponse errorResponse = getErrorResponse(response);

        assertEquals(400, response.getStatusLine().getStatusCode());

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error source", "timestamp", errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid error message", "Out of bounds. Cannot be more than 259200000 milliseconds into the past." +
                " Cannot be more than 600000 milliseconds into the future", errorResponse.getErrors().get(0).getMessage());
    }

    @Test
    public void testHttpEnumIngestionInvalidFutureCollectionTime() throws IOException, URISyntaxException {

        long timestamp = System.currentTimeMillis() + TIME_DIFF_MS + Configuration.getInstance().getLongProperty( CoreConfig.AFTER_CURRENT_COLLECTIONTIME_MS );

        // ingest and rollup metrics with enum values and verify CF points and elastic search indexes
        final String tenant_id = "333333";

        String postfix = getPostfix();

        // post enum metric for ingestion and verify
        HttpResponse response = postMetric(tenant_id, postAggregatedPath, "sample_enums_payload.json", timestamp, postfix );
        ErrorResponse errorResponse = getErrorResponse(response);

        assertEquals(400, response.getStatusLine().getStatusCode());

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error source", "timestamp", errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid error message", "Out of bounds. Cannot be more than 259200000 milliseconds into the past." +
                " Cannot be more than 600000 milliseconds into the future", errorResponse.getErrors().get(0).getMessage());
    }

    private ErrorResponse getErrorResponse(HttpResponse response) throws IOException {
        return new ObjectMapper().readValue(response.getEntity().getContent(), ErrorResponse.class);
    }
}
