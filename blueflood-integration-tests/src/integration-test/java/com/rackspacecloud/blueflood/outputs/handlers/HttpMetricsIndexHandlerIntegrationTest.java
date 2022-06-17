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

import static junit.framework.Assert.assertEquals;

import com.rackspacecloud.blueflood.http.HttpIntegrationTestBase;
import com.rackspacecloud.blueflood.io.IOContainer;
import com.rackspacecloud.blueflood.io.MetricsRW;
import com.rackspacecloud.blueflood.outputs.formats.ErrorResponse;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.TimeValue;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration Tests for GET .../metrics/search
 */
public class HttpMetricsIndexHandlerIntegrationTest extends HttpIntegrationTestBase {

    private final String tenantId = "540123";
    private final String metricPrefix = "test.search";
    private final int numMetrics = 5;

    @Before
    public void setup() throws Exception {

        super.setUp();

        // setup metrics to be searchable
        MetricsRW metricsRW = IOContainer.fromConfig().getBasicMetricsRW();

        final List<IMetric> metrics = new ArrayList<IMetric>();
        for (int i = 0; i < numMetrics; i++) {
            long curMillis = baseMillis + i;
            Locator locator = Locator.createLocatorFromPathComponents(tenantId, metricPrefix + "." + i);
            Metric metric = new Metric(locator, getRandomIntMetricValue(), curMillis, new TimeValue(1, TimeUnit.DAYS), locatorToUnitMap.get(locator));
            metrics.add(metric);
        }

        elasticIO.insertDiscovery(new ArrayList<IMetric>(metrics));
        esSetup.client().admin().indices().prepareRefresh().execute().actionGet();

        metricsRW.insertMetrics(metrics);
    }

    @Test
    public void testHttpMetricsIndexHandler_HappyCaseAllResults() throws Exception {
        parameterMap = new HashMap<String, String>();
        parameterMap.put("query", metricPrefix + ".*");
        HttpGet get = new HttpGet(getQuerySearchURI(tenantId));
        HttpResponse response = client.execute(get);

        String responseString = EntityUtils.toString(response.getEntity());
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        for (int i = 0; i < numMetrics; i++){
            Assert.assertTrue(responseString.contains(String.format("{\"metric\":\"%s.%d\"}", metricPrefix, i)));
        }
        assertResponseHeaderAllowOrigin(response);
    }

    @Test
    public void testHttpMetricsIndexHandler_HappyCaseNoResults() throws Exception {
        parameterMap = new HashMap<String, String>();
        parameterMap.put("query", "dne." + baseMillis + "." + getPostfix());
        HttpGet get = new HttpGet(getQuerySearchURI(tenantId));
        HttpResponse response = client.execute(get);

        String responseString = EntityUtils.toString(response.getEntity());
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        Assert.assertEquals("[]", responseString);
        assertResponseHeaderAllowOrigin(response);
    }

    @Test
    public void testHttpMetricsIndexHandler_NoQueryParam() throws Exception {
        parameterMap = new HashMap<String, String>();
        HttpGet get = new HttpGet(getQuerySearchURI(tenantId));
        HttpResponse response = client.execute(get);

        ErrorResponse errorResponse = getErrorResponse(response);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "Invalid Query String", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid tenant", tenantId, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid metric name", "", errorResponse.getErrors().get(0).getMetricName());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST.code(), response.getStatusLine().getStatusCode());
    }

    @Test
    public void testHttpMetricsIndexHandler_InvalidQueryParam() throws Exception {
        parameterMap = new HashMap<String, String>();
        parameterMap.put("query", "query ! with bad \\ characters ? <>");
        HttpGet get = new HttpGet(getQuerySearchURI(tenantId));
        HttpResponse response = client.execute(get);

        Assert.assertEquals(500, response.getStatusLine().getStatusCode());
        assertResponseHeaderAllowOrigin(response);
    }

    private ErrorResponse getErrorResponse(HttpResponse response) throws IOException {
        return new ObjectMapper().readValue(response.getEntity().getContent(), ErrorResponse.class);
    }
}
