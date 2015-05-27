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
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
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

public class HttpRollupHandlerIntegrationTest extends IntegrationTestBase {
    // A timestamp 2 days ago
    private final long baseMillis = Calendar.getInstance().getTimeInMillis() - 172800000;
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
    public static void setUpHttp() {
        queryPort = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_METRIC_DATA_QUERY_PORT);
        httpQueryService = new HttpQueryService();
        httpQueryService.startService();
        vendor = new HttpClientVendor();
        client = vendor.getClient();
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        AstyanaxWriter writer = AstyanaxWriter.getInstance();
        IncomingMetricMetadataAnalyzer analyzer = new IncomingMetricMetadataAnalyzer(MetadataCache.getInstance());

        // insert something every 1m for 24h
        for (int i = 0; i < 1440; i++) {
            final long curMillis = baseMillis + (i * 60000);
            final List<Metric> metrics = new ArrayList<Metric>();
            final Metric metric = getRandomIntMetric(locators[0], curMillis);
            final Metric stringMetric = getRandomStringmetric(locators[1], curMillis);
            metrics.add(metric);
            metrics.add(stringMetric);

            analyzer.scanMetrics(new ArrayList<IMetric>(metrics));
            writer.insertFull(metrics);
        }

        httpHandler = new HttpRollupsQueryHandler();

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
        testGetRollupByResolution();
        testHttpRequestForPoints();
        testHttpRequestForHistograms();
    }

    private void testGetRollupByPoints() throws Exception {
        final Map<Granularity, Integer> points = new HashMap<Granularity, Integer>();
        points.put(Granularity.FULL, 1600);
        points.put(Granularity.MIN_5, 287);
        points.put(Granularity.MIN_20, 71);
        points.put(Granularity.MIN_60, 23);
        points.put(Granularity.MIN_240, 5);
        points.put(Granularity.MIN_1440, 1);

        testHTTPRollupHandlerGetByPoints(locatorToPoints, points, baseMillis, baseMillis + 86400000);
    }

    private void testGetRollupByResolution() throws Exception {
        for (Locator locator : locators) {
            for (Resolution resolution : Resolution.values()) {
                Granularity g = Granularity.granularities()[resolution.getValue()];
                testHTTPHandlersGetByResolution(locator, resolution, baseMillis, baseMillis + 86400000,
                        locatorToPoints.get(locator).get(g));
            }
        }
    }

    private void testHTTPRollupHandlerGetByPoints(Map<Locator, Map<Granularity, Integer>> answers, Map<Granularity, Integer> points,
                                                   long from, long to) throws Exception {
        for (Locator locator : locators) {
            for (Granularity g2 : Granularity.granularities()) {
                MetricData data = httpHandler.GetDataByPoints(
                        locator.getTenantId(),
                        locator.getMetricName(),
                        baseMillis,
                        baseMillis + 86400000,
                        points.get(g2));
                Assert.assertEquals((int) answers.get(locator).get(g2), data.getData().getPoints().size());
		// Disabling test that fail on ES
                // Assert.assertEquals(locatorToUnitMap.get(locator), data.getUnit());
            }
        }
    }

    private void testHTTPHandlersGetByResolution(Locator locator, Resolution resolution, long from, long to,
                                                 int expectedPoints) throws Exception {
        Assert.assertEquals(expectedPoints, getNumberOfPointsViaHTTPHandler(httpHandler, locator,
                from, to, resolution));
    }

    private int getNumberOfPointsViaHTTPHandler(HttpRollupsQueryHandler handler,
                                               Locator locator, long from, long to, Resolution resolution)
            throws Exception {
        final MetricData values = handler.GetDataByResolution(locator.getTenantId(),
                locator.getMetricName(), from, to, resolution);
        return values.getData().getPoints().size();
    }

    private void testHttpRequestForPoints() throws Exception {
        testHappyCaseHTTPRequest();
        testBadRequest();
        testBadMethod();
        testHappyCaseMultiFetchHTTPRequest();
    }

    private void testHappyCaseHTTPRequest() throws Exception {
        HttpGet get = new HttpGet(getMetricsQueryURI());
        HttpResponse response = client.execute(get);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private void testHttpRequestForHistograms() throws Exception {
        HttpGet get = new HttpGet(getHistQueryURI());
        HttpResponse response = client.execute(get);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private void testBadRequest() throws Exception {
        HttpGet get = new HttpGet(getInvalidMetricsQueryURI());
        HttpResponse response = client.execute(get);
        Assert.assertEquals(400, response.getStatusLine().getStatusCode());
    }

    private void testBadMethod() throws Exception {
        HttpPost post = new HttpPost(getMetricsQueryURI());
        HttpResponse response = client.execute(post);
        Assert.assertEquals(405, response.getStatusLine().getStatusCode());
    }

    private void testHappyCaseMultiFetchHTTPRequest() throws Exception {
        HttpPost post = new HttpPost(getBatchMetricsQueryURI());
        JSONArray metricsToGet = new JSONArray();
        for (Locator locator : locators) {
            metricsToGet.add(locator.toString());
        }
        HttpEntity entity = new StringEntity(metricsToGet.toString(), ContentType.APPLICATION_JSON);
        post.setEntity(entity);
        HttpResponse response = client.execute(post);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private URI getMetricsQueryURI() throws URISyntaxException {
        URIBuilder builder = new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(queryPort).setPath("/v2.0/" + tenantId + "/views/" + metricName)
                .setParameter("from", String.valueOf(baseMillis))
                .setParameter("to", String.valueOf(baseMillis + 86400000))
                .setParameter("resolution", "full");
        return builder.build();
    }

    private URI getHistQueryURI() throws URISyntaxException {
        URIBuilder builder = new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(queryPort).setPath("/v2.0/" + tenantId + "/views/histograms/" + metricName)
                .setParameter("from", String.valueOf(baseMillis))
                .setParameter("to", String.valueOf(baseMillis + 86400000))
                .setParameter("resolution", "full");
        return builder.build();
    }

    private URI getBatchMetricsQueryURI() throws Exception {
        URIBuilder builder = new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(queryPort).setPath("/v2.0/" + tenantId + "/views")
                .setParameter("from", String.valueOf(baseMillis))
                .setParameter("to", String.valueOf(baseMillis + 86400000))
                .setParameter("resolution", "full");
        return builder.build();
    }

    private URI getInvalidMetricsQueryURI() throws URISyntaxException {
        URIBuilder builder = new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(queryPort).setPath("/v2.0/" + tenantId + "/views/" + metricName)
                .setParameter("from", String.valueOf(baseMillis))
                .setParameter("resolution", "full");  // Misses parameter 'to'
        return builder.build();
    }

    @AfterClass
    public static void shutdown() {
        vendor.shutdown();
        httpQueryService.stopService();
    }
}
