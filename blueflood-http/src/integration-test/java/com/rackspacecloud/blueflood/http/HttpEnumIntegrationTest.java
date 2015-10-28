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

import com.rackspacecloud.blueflood.service.EnumValidator;
import com.rackspacecloud.blueflood.types.Locator;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class HttpEnumIntegrationTest extends HttpIntegrationTestBase {

    @Test
    public void testMetricIngestionWithEnum() throws Exception {
        // ingest and rollup metrics with enum values and verify CF points and elastic search indexes
        final String tenant_id = "333333";
        final String metric_name = "enum_metric_test";
        Set<Locator> locators = new HashSet<Locator>();
        locators.add(Locator.createLocatorFromPathComponents(tenant_id, metric_name));

        // post enum metric for ingestion and verify
        HttpResponse response = postMetric(tenant_id, postAggregatedPath, "src/test/resources/sample_enums_payload.json");
        Assert.assertEquals("Should get status 200 from ingestion server for POST", 200, response.getStatusLine().getStatusCode());
        EntityUtils.consume(response.getEntity());

        // execute EnumValidator
        EnumValidator enumValidator = new EnumValidator(locators);
        enumValidator.run();

        //Sleep for a while
        Thread.sleep(3000);

        // query for metric and assert results
        HttpResponse query_response = queryMetricIncludeEnum(tenant_id, metric_name);
        Assert.assertEquals("Should get status 200 from query server for GET", 200, query_response.getStatusLine().getStatusCode());

        // assert response content
        String responseContent = EntityUtils.toString(query_response.getEntity(), "UTF-8");
        Assert.assertEquals(String.format("[{\"metric\":\"%s\",\"enum_values\":[\"v1\",\"v2\",\"v3\"]}]", metric_name), responseContent);
        EntityUtils.consume(query_response.getEntity());
    }

}
