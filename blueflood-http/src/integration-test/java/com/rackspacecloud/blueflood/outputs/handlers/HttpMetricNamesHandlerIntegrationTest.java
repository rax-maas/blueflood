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

import com.github.tlrx.elasticsearch.test.EsSetup;
import com.rackspacecloud.blueflood.http.HttpIntegrationTestBase;
import com.rackspacecloud.blueflood.io.ElasticIO;
import com.rackspacecloud.blueflood.outputs.formats.ErrorResponse;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.TimeValue;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.rackspacecloud.blueflood.io.AbstractElasticIO.ELASTICSEARCH_INDEX_NAME_READ;
import static junit.framework.Assert.assertEquals;

/**
 * Integration Tests for GET .../metric_name/search
 */
public class HttpMetricNamesHandlerIntegrationTest extends HttpIntegrationTestBase {

    private static final String tenantId = "540123";

    private static final String metricPrefix = "test.token.search";
    private static final int numMetrics = 5;

    @BeforeClass
    public static void setup() throws Exception {
        esSetup = new EsSetup();

        final List<IMetric> metrics = new ArrayList<>();
        for (int i = 0; i < numMetrics; i++) {
            long curMillis = baseMillis + i;
            Locator locator = Locator.createLocatorFromPathComponents(tenantId, metricPrefix + "." + i);
            Metric metric = new Metric(locator, RAND.nextInt(),
                    curMillis, new TimeValue(1, TimeUnit.DAYS), locatorToUnitMap.get(locator));
            metrics.add(metric);
        }

        elasticIO = new ElasticIO();
        elasticIO.insertDiscovery(metrics);

        int statusCode = elasticIO.elasticsearchRestHelper.refreshIndex(ELASTICSEARCH_INDEX_NAME_READ);
        if(statusCode != 200) {
            System.out.println(String.format("Refresh for %s failed with status code: %d",
                    ELASTICSEARCH_INDEX_NAME_READ, statusCode));
        }
    }

    /*
    Once done testing, delete all of the records of the given type and index.
    NOTE: Don't delete the index or the type, because that messes up the ES settings.
     */
    @AfterClass
    public static void tearDownClass() throws Exception {
        URIBuilder builder = new URIBuilder().setScheme("http")
                .setHost("127.0.0.1").setPort(9200)
                .setPath("/metric_metadata/metrics/_query");

        HttpEntityEnclosingRequestBase delete = new HttpEntityEnclosingRequestBase() {
            @Override
            public String getMethod() {
                return "DELETE";
            }
        };
        delete.setURI(builder.build());

        String deletePayload = "{\"query\":{\"match_all\":{}}}";
        HttpEntity entity = new NStringEntity(deletePayload, ContentType.APPLICATION_JSON);
        delete.setEntity(entity);

        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = client.execute(delete);
        if(response.getStatusLine().getStatusCode() != 200)
        {
            System.out.println("Couldn't delete index after running tests.");
        }
        else {
            System.out.println("Successfully deleted index after running tests.");
        }
    }

    @Test
    public void testHttpMetricNamesHandler_HappyCaseAllResults() throws Exception {
        parameterMap = new HashMap<>();
        parameterMap.put("query", metricPrefix + ".*");
        HttpGet get = new HttpGet(getMetricNameSearchURI(tenantId));
        HttpResponse response = client.execute(get);

        String responseString = EntityUtils.toString(response.getEntity());
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        for (int i = 0; i < numMetrics; i++){
            Assert.assertTrue(responseString.contains(String.format("{\"%s.%d\":true}", metricPrefix, i)));
        }
        assertResponseHeaderAllowOrigin(response);
    }

    @Test
    public void testHttpMetricNamesHandler_HappyCaseNoResults() throws Exception {
        parameterMap = new HashMap<>();
        parameterMap.put("query","dne." + baseMillis + "." + getPostfix());
        HttpGet get = new HttpGet(getMetricNameSearchURI(tenantId));
        HttpResponse response = client.execute(get);

        String responseString = EntityUtils.toString(response.getEntity());
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        Assert.assertEquals("[]", responseString);
        assertResponseHeaderAllowOrigin(response);
    }

    @Test
    public void testHttpMetricNamesHandler_NoQueryParam() throws Exception {
        parameterMap = new HashMap<>();
        HttpGet get = new HttpGet(getMetricNameSearchURI(tenantId));
        HttpResponse response = client.execute(get);

        ErrorResponse errorResponse = getErrorResponse(response);

        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "Invalid Query String", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid tenant", tenantId, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST.code(), response.getStatusLine().getStatusCode());
    }

    private ErrorResponse getErrorResponse(HttpResponse response) throws IOException {
        return new ObjectMapper().readValue(response.getEntity().getContent(), ErrorResponse.class);
    }
}
