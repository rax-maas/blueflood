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
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.outputs.formats.ErrorResponse;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.Token;
import com.rackspacecloud.blueflood.utils.ModuleLoader;
import com.rackspacecloud.blueflood.utils.TimeValue;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Integration Tests for GET .../metric_name/search
 *
 * The current scope gives us one cluster for all test methods in the test.
 * All indices and templates are deleted between each test.
 *
 * The following flags have to be set while running this test
 * -Dtests.jarhell.check=false (to handle some bug in intellij https://github.com/elastic/elasticsearch/issues/14348)
 * -Dtests.security.manager=false (https://github.com/elastic/elasticsearch/issues/16459)
 *
 */
public class HttpMetricNamesHandlerIntegrationTest extends HttpIntegrationTestBase {

    private final String tenantId = "540123";

    private final String metricPrefix = "test.token.search";
    private final int numMetrics = 5;

    @Before
    public void setup() throws Exception {

        // setup elasticsearch test clusters with blueflood mappings
        createIndexAndMapping(ElasticIO.ELASTICSEARCH_INDEX_NAME_WRITE,
                              ElasticIO.ES_DOCUMENT_TYPE,
                              getMetricsMapping());

        createIndexAndMapping(ElasticTokensIO.ELASTICSEARCH_TOKEN_INDEX_NAME_WRITE,
                              ElasticTokensIO.ES_DOCUMENT_TYPE,
                              getTokensMapping());

        // create elasticsearch client and link it to ModuleLoader
        elasticIO = new ElasticIO(getClient());
        elasticTokensIO = new ElasticTokensIO(getClient());

        ((ElasticIO) ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.DISCOVERY_MODULES)).setClient(getClient());
        ((ElasticTokensIO) ModuleLoader.getInstance(TokenDiscoveryIO.class, CoreConfig.TOKEN_DISCOVERY_MODULES)).setClient(getClient());

        // setup metrics to be searchable
        MetricsRW metricsRW = IOContainer.fromConfig().getBasicMetricsRW();

        final List<IMetric> metrics = new ArrayList<IMetric>();
        for (int i = 0; i < numMetrics; i++) {
            long curMillis = baseMillis + i;
            Locator locator = Locator.createLocatorFromPathComponents(tenantId, metricPrefix + "." + i);
            Metric metric = new Metric(locator, getRandomIntMetricValue(), curMillis, new TimeValue(1, TimeUnit.DAYS), getLocatorToUnitMap().get(locator));
            metrics.add(metric);
        }

        elasticIO.insertDiscovery(new ArrayList<IMetric>(metrics));

        Stream<Locator> locators = metrics.stream().map(IMetric::getLocator);
        elasticTokensIO.insertDiscovery(Token.getUniqueTokens(locators).collect(toList()));
        refreshChanges();

        metricsRW.insertMetrics(metrics);
    }

    @Test
    public void testHttpMetricNamesHandler_HappyCaseAllResults() throws Exception {
        parameterMap = new HashMap<String, String>();
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
        parameterMap = new HashMap<String, String>();
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
        parameterMap = new HashMap<String, String>();
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
