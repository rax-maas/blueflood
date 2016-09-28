/*
 * Copyright 2013 Rackspace
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

import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.http.HttpClientVendor;
import com.rackspacecloud.blueflood.http.HttpIntegrationTestBase;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.*;
import com.rackspacecloud.blueflood.types.*;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.JSONArray;
import org.junit.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class HttpRollupHandlerIntegrationTest extends HttpIntegrationTestBase {
    // A timestamp 2 days ago
    private static final long baseMillis = Calendar.getInstance().getTimeInMillis() - 172800000;
    private final String tenantId = "ac" + IntegrationTestBase.randString(8);
    private final String metricName = "met_" + IntegrationTestBase.randString(8);
    private final String strMetricName = "strMet_" + IntegrationTestBase.randString(8);
    final Locator[] locators = new Locator[] {
            Locator.createLocatorFromPathComponents(tenantId, metricName),
            Locator.createLocatorFromPathComponents(tenantId, strMetricName)
    };
    private static int queryPort = 20000;
    private static HttpQueryService httpQueryService;
    private static HttpClientVendor vendor;
    private static DefaultHttpClient client;

    private HttpRollupsQueryHandler httpHandler;
    private final Map<Locator, Map<Granularity, Integer>> locatorToPoints = new HashMap<Locator, Map<Granularity,Integer>>();

    @BeforeClass
    public static void setUpHttp() throws Exception {
        // this method suppress the @BeforeClass method of the parent
        queryPort = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_METRIC_DATA_QUERY_PORT);
        httpQueryService = new HttpQueryService();
        httpQueryService.startService();
        vendor = new HttpClientVendor();
        client = vendor.getClient();
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MetricsRW metricsRW = IOContainer.fromConfig().getBasicMetricsRW();
        IncomingMetricMetadataAnalyzer analyzer = new IncomingMetricMetadataAnalyzer(MetadataCache.getInstance());
        httpHandler = new HttpRollupsQueryHandler();

        // insert something every 1m for 24h
        for (int i = 0; i < 1440; i++) {
            final long curMillis = baseMillis + (i * 60000);
            final List<IMetric> metrics = new ArrayList<IMetric>();
            final Metric metric = getRandomIntMetric(locators[0], curMillis);
            final Metric stringMetric = getRandomStringmetric(locators[1], curMillis);
            metrics.add(metric);
            metrics.add(stringMetric);

            analyzer.scanMetrics(new ArrayList<IMetric>(metrics));
            metricsRW.insertMetrics(metrics);
        }

        // generate every level of rollup for the raw data
        Granularity g = Granularity.FULL;
        while (g != Granularity.MIN_1440) {
            g = g.coarser();
            for (Locator locator : locators) {
                generateRollups(locator, baseMillis, baseMillis + 86400000, g);
            }
        }

        final Map<Granularity, Integer> answerForNumericMetric = new HashMap<Granularity, Integer>();
        answerForNumericMetric.put(Granularity.FULL, 1440);
        answerForNumericMetric.put(Granularity.MIN_5, 289);
        answerForNumericMetric.put(Granularity.MIN_20, 73);
        answerForNumericMetric.put(Granularity.MIN_60, 25);
        answerForNumericMetric.put(Granularity.MIN_240, 7);
        answerForNumericMetric.put(Granularity.MIN_1440, 2);

        final Map<Granularity, Integer> answerForStringMetric = new HashMap<Granularity, Integer>();
        answerForStringMetric.put(Granularity.FULL, 1440);
        answerForStringMetric.put(Granularity.MIN_5, 1440);
        answerForStringMetric.put(Granularity.MIN_20, 1440);
        answerForStringMetric.put(Granularity.MIN_60, 1440);
        answerForStringMetric.put(Granularity.MIN_240, 1440);
        answerForStringMetric.put(Granularity.MIN_1440, 1440);

        locatorToPoints.put(locators[0], answerForNumericMetric);
        locatorToPoints.put(locators[1], answerForStringMetric);
    }

    @Test
    public void testGetPoints() throws Exception {
        testGetRollupByPoints();
        checkGetRollupByResolution(Arrays.asList(locators), locatorToPoints, baseMillis, httpHandler);
        testHttpRequestForPoints();
    }

    private void testGetRollupByPoints() throws Exception {
        final Map<Granularity, Integer> points = new HashMap<Granularity, Integer>();
        points.put(Granularity.FULL, 1600);
        points.put(Granularity.MIN_5, 287);
        points.put(Granularity.MIN_20, 71);
        points.put(Granularity.MIN_60, 23);
        points.put(Granularity.MIN_240, 5);
        points.put(Granularity.MIN_1440, 1);

        checkHttpRollupHandlerGetByPoints(locatorToPoints, points, baseMillis, baseMillis + 86400000, Arrays.asList(locators), httpHandler);
    }

    private void testHttpRequestForPoints() throws Exception {
        testHappyCaseHTTPRequest(metricName, tenantId, client);
        testBadRequest(metricName, tenantId, client);
        testBadMethod(metricName, tenantId, client);
        testHappyCaseMultiFetchHTTPRequest(Arrays.asList(locators), tenantId, client);
    }

    public static void testHappyCaseHTTPRequest(String metricName, String tenantId, DefaultHttpClient client) throws Exception {
        HttpGet get = new HttpGet(getMetricsQueryURI(metricName, tenantId));
        HttpResponse response = client.execute(get);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    }

    public static void testBadRequest(String metricName, String tenantId, DefaultHttpClient client) throws Exception {
        HttpGet get = new HttpGet(getInvalidMetricsQueryURI(metricName, tenantId));
        HttpResponse response = client.execute(get);
        Assert.assertEquals(400, response.getStatusLine().getStatusCode());
    }

    public static void testBadMethod(String metricName, String tenantId, DefaultHttpClient client) throws Exception {
        HttpPost post = new HttpPost(getMetricsQueryURI(metricName, tenantId));
        HttpResponse response = client.execute(post);
        Assert.assertEquals(405, response.getStatusLine().getStatusCode());
    }

    public static void testHappyCaseMultiFetchHTTPRequest(List<Locator> locators, String tenantId, DefaultHttpClient client) throws Exception {
        HttpPost post = new HttpPost(getBatchMetricsQueryURI(tenantId));
        JSONArray metricsToGet = new JSONArray();
        for (Locator locator : locators) {
            metricsToGet.add(locator.toString());
        }
        HttpEntity entity = new StringEntity(metricsToGet.toString(), ContentType.APPLICATION_JSON);
        post.setEntity(entity);
        HttpResponse response = client.execute(post);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private static URI getMetricsQueryURI(String metricName, String tenantId) throws URISyntaxException {
        URIBuilder builder = new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(queryPort).setPath("/v2.0/" + tenantId + "/views/" + metricName)
                .setParameter("from", String.valueOf(baseMillis))
                .setParameter("to", String.valueOf(baseMillis + 86400000))
                .setParameter("resolution", "full");
        return builder.build();
    }

    private static URI getBatchMetricsQueryURI(String tenantId) throws Exception {
        URIBuilder builder = new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(queryPort).setPath("/v2.0/" + tenantId + "/views")
                .setParameter("from", String.valueOf(baseMillis))
                .setParameter("to", String.valueOf(baseMillis + 86400000))
                .setParameter("resolution", "full");
        return builder.build();
    }

    private static URI getInvalidMetricsQueryURI(String metricName, String tenantId) throws URISyntaxException {
        URIBuilder builder = new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(queryPort).setPath("/v2.0/" + tenantId + "/views/" + metricName)
                .setParameter("from", String.valueOf(baseMillis))
                .setParameter("resolution", "full");  // Misses parameter 'to'
        return builder.build();
    }

    @AfterClass
    public static void shutdown() {
        if (vendor != null) {
            vendor.shutdown();
        }

        if (httpQueryService != null) {
            httpQueryService.stopService();
        }
    }
}
