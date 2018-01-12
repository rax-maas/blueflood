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

import com.rackspacecloud.blueflood.http.HttpIntegrationTestBase;
import com.rackspacecloud.blueflood.io.ElasticIO;
import com.rackspacecloud.blueflood.io.EventElasticSearchIO;
import com.rackspacecloud.blueflood.io.IOContainer;
import com.rackspacecloud.blueflood.io.MetricsRW;
import com.rackspacecloud.blueflood.outputs.formats.ErrorResponse;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.TimeValue;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Integration Tests for GET .../metrics/search
 *
 * The current scope gives us one cluster for all test methods in the test.
 * All indices and templates are deleted between each test.
 *
 * The following flags have to be set while running this test
 * -Dtests.jarhell.check=false (to handle some bug in intellij https://github.com/elastic/elasticsearch/issues/14348)
 * -Dtests.security.manager=false (https://github.com/elastic/elasticsearch/issues/16459)
 *
 */
public class HttpMetricsIndexHandlerIntegrationTest extends HttpIntegrationTestBase {

    private final String tenantId = "540123";
    private final String metricPrefix = "test.search";
    private final int numMetrics = 5;

    @Before
    public void setup() throws Exception {

        super.esSetup();
        ((EventElasticSearchIO) eventsSearchIO).setClient(getClient());

        // setup metrics to be searchable
        MetricsRW metricsRW = IOContainer.fromConfig().getBasicMetricsRW();

        final List<IMetric> metrics = new ArrayList<IMetric>();
        for (int i = 0; i < numMetrics; i++) {
            long curMillis = baseMillis + i;
            Locator locator = Locator.createLocatorFromPathComponents(tenantId, metricPrefix + "." + i);
            Metric metric = new Metric(locator, getRandomIntMetricValue(), curMillis, new TimeValue(1, TimeUnit.DAYS), getLocatorToUnitMap().get(locator));
            metrics.add(metric);
        }

        // create elasticsearch client and link it to ModuleLoader
        elasticIO = new ElasticIO(getClient());

        elasticIO.insertDiscovery(new ArrayList<IMetric>(metrics));
        refreshChanges();

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
