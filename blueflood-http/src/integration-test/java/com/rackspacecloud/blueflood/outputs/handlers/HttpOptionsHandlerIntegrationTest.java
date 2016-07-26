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
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpOptions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

/**
 * Integration Tests for OPTIONS
 */
public class HttpOptionsHandlerIntegrationTest extends HttpIntegrationTestBase {

    private final String tenantId = "100000";
    private String[] allowedOrigins;
    private String[] allowedHeaders;
    private String[] allowedMethods;
    private String allowedMaxAge;

    @Before
    public void setUp() {
        parameterMap = new HashMap<String, String>();
        allowedOrigins = Configuration.getInstance().getStringProperty(CoreConfig.CORS_ALLOWED_ORIGINS).split(",");
        allowedHeaders = Configuration.getInstance().getStringProperty(CoreConfig.CORS_ALLOWED_HEADERS).split(",");
        allowedMethods = Configuration.getInstance().getStringProperty(CoreConfig.CORS_ALLOWED_METHODS).split(",");
        allowedMaxAge = Configuration.getInstance().getStringProperty(CoreConfig.CORS_ALLOWED_MAX_AGE);
    }

    @Test
    public void testHttpEventsQueryHandlerOptions() throws Exception {
        // test query .../events/getEvents for CORS support
        HttpOptions httpOptions = new HttpOptions(getQueryEventsURI(tenantId));
        HttpResponse response = client.execute(httpOptions);
        assertCorsResponseHeaders(response, allowedOrigins, allowedHeaders, allowedMethods, allowedMaxAge);
    }

    @Test
    public void testHttpMetricsIndexHandlerOptions() throws Exception {
        // test query .../metrics/search for CORS support
        HttpOptions httpOptions = new HttpOptions(getQuerySearchURI(tenantId));
        HttpResponse response = client.execute(httpOptions);
        assertCorsResponseHeaders(response, allowedOrigins, allowedHeaders, allowedMethods, allowedMaxAge);
    }

    @Test
    public void testHttpMetricTokensHandlerOptions() throws Exception {
        // test query .../metric_name/search for CORS support
        HttpOptions httpOptions = new HttpOptions(getQueryTokenSearchURI(tenantId));
        HttpResponse response = client.execute(httpOptions);
        assertCorsResponseHeaders(response, allowedOrigins, allowedHeaders, allowedMethods, allowedMaxAge);
    }

    @Test
    public void testHttpMultiRollupsQueryHandlerOptions() throws Exception {
        // test query .../views for CORS support
        HttpOptions httpOptions = new HttpOptions(getQueryViewsURI(tenantId));
        HttpResponse response = client.execute(httpOptions);
        assertCorsResponseHeaders(response, allowedOrigins, allowedHeaders, allowedMethods, allowedMaxAge);
    }

    @Test
    public void testHttpRollupsQueryHandlerOptions() throws Exception {
        // test query .../views/metric_name for CORS support
        HttpOptions httpOptions = new HttpOptions(getQueryMetricViewsURI(tenantId, "test.cors.metric"));
        HttpResponse response = client.execute(httpOptions);
        assertCorsResponseHeaders(response, allowedOrigins, allowedHeaders, allowedMethods, allowedMaxAge);
    }

    private void assertCorsResponseHeaders(HttpResponse response,
                                           String[] allowedOrigins,
                                           String[] allowedHeaders,
                                           String[] allowedMethods,
                                           String allowedMaxAge) {

        // assert response code and empty entity
        Assert.assertEquals(204, response.getStatusLine().getStatusCode());
        Assert.assertNull(response.getEntity());

        // assert allowed origins
        Header[] allowOriginResponse = response.getHeaders("Access-Control-Allow-Origin");
        Assert.assertTrue("Missing allow origin in response", allowOriginResponse.length > 0);

        String actualAllowOrigin = allowOriginResponse[0].getValue();
        Assert.assertEquals("Invalid number of allowed origins: " + actualAllowOrigin, allowedOrigins.length, actualAllowOrigin.split(",").length);
        for (String allowedOrigin : allowedOrigins) {
            Assert.assertTrue("Missing allowed origin " + allowedOrigin, actualAllowOrigin.contains(allowedOrigin));
        }

        // assert allowed headers
        Header[] allowHeadersResponse = response.getHeaders("Access-Control-Allow-Headers");
        Assert.assertTrue("Missing allow headers in response", allowHeadersResponse.length > 0);

        String actualAllowHeaders = allowHeadersResponse[0].getValue();
        Assert.assertEquals("Invalid number of allowed headers: " + actualAllowHeaders, allowedHeaders.length, actualAllowHeaders.split(",").length);
        for (String allowedHeader : allowedHeaders) {
            Assert.assertTrue("Missing allowed header " + allowedHeader, actualAllowHeaders.contains(allowedHeader));
        }

        // assert allowed methods
        Header[] allowMethodsResponse = response.getHeaders("Access-Control-Allow-Methods");
        Assert.assertTrue("Missing allow methods in response", allowMethodsResponse.length > 0);

        String actualAllowMethods = allowMethodsResponse[0].getValue();
        Assert.assertEquals("Invalid number of allowed methods: " + actualAllowMethods, allowedMethods.length, actualAllowMethods.split(",").length);
        for (String allowedMethod : allowedMethods) {
            Assert.assertTrue("Missing allowed method " + allowedMethod, actualAllowMethods.contains(allowedMethod));
        }

        // assert allowed max age
        Header[] allowMaxAgeResponse = response.getHeaders("Access-Control-Max-Age");
        Assert.assertTrue("Missing allow max age in response", allowMaxAgeResponse.length > 0);
        Assert.assertEquals("Allowed max age does not match", allowedMaxAge, allowMaxAgeResponse[0].getValue());
    }

}
