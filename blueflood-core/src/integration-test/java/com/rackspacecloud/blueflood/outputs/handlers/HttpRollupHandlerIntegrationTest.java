package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.http.HttpClientVendor;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.types.*;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.JSONArray;
import org.junit.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpRollupHandlerIntegrationTest extends IntegrationTestBase {
    private final long baseMillis = 1335820166000L;
    private final String tenantId = "ac" + IntegrationTestBase.randString(8);
    private final String metricName = "met_" + IntegrationTestBase.randString(8);
    final Locator[] locators = new Locator[] {
            Locator.createLocatorFromPathComponents(tenantId, metricName)
    };
    private static int queryPort = 20000;
    private static HttpMetricDataQueryServer httpMetricDataQueryServer;
    private static HttpClientVendor vendor;
    private static DefaultHttpClient client;

    private HttpRollupsQueryHandler httpHandler;

    @BeforeClass
    public static void setUpHttp() {
        queryPort = Configuration.getIntegerProperty("HTTP_METRIC_DATA_QUERY_PORT");
        httpMetricDataQueryServer = new HttpMetricDataQueryServer(queryPort);
        vendor = new HttpClientVendor();
        client = vendor.getClient();
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        AstyanaxWriter writer = AstyanaxWriter.getInstance();

        // insert something every 1m for 24h
        for (int i = 0; i < 1440; i++) {
            final long curMillis = baseMillis + i * 60000;
            final List<Metric> metrics = new ArrayList<Metric>();
            final Metric metric = getRandomIntMetric(locators[0], curMillis);
            metrics.add(metric);

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
    }

    @Test
    public void testGetPoints() throws Exception {
        testGetRollupByPoints();
        testGetRollupByResolution();
        testHttpRequestForPoints();
    }

    private void testGetRollupByPoints() throws Exception {
        final Map<String, Integer> answers = new HashMap<String, Integer>();
        answers.put("all of them", 1440);
        answers.put(Granularity.FULL.name(), 289);
        answers.put(Granularity.MIN_5.name(), 289);
        answers.put(Granularity.MIN_20.name(), 73);
        answers.put(Granularity.MIN_60.name(), 25);
        answers.put(Granularity.MIN_240.name(), 7);
        answers.put(Granularity.MIN_1440.name(), 2);

        final Map<String, Integer> points = new HashMap<String, Integer>();
        points.put("all of them", 1700);
        points.put(Granularity.FULL.name(), 800);
        points.put(Granularity.MIN_5.name(), 287);
        points.put(Granularity.MIN_20.name(), 71);
        points.put(Granularity.MIN_60.name(), 23);
        points.put(Granularity.MIN_240.name(), 5);
        points.put(Granularity.MIN_1440.name(), 1);

        testHTTPRollupHandlerGetByPoints(answers, points, baseMillis, baseMillis + 86400000);
    }

    private void testGetRollupByResolution() throws Exception {
        for (Locator locator : locators) {
            testHTTPHandlersGetByResolution(locator, Resolution.FULL, baseMillis, baseMillis + 86400000, 1440);
            testHTTPHandlersGetByResolution(locator, Resolution.MIN5, baseMillis, baseMillis + 86400000, 289);
            testHTTPHandlersGetByResolution(locator, Resolution.MIN20, baseMillis, baseMillis + 86400000, 73);
            testHTTPHandlersGetByResolution(locator, Resolution.MIN60, baseMillis, baseMillis + 86400000, 25);
            testHTTPHandlersGetByResolution(locator, Resolution.MIN240, baseMillis, baseMillis + 86400000, 7);
            testHTTPHandlersGetByResolution(locator, Resolution.MIN1440, baseMillis, baseMillis + 86400000, 2);
        }
    }

    private void testHTTPRollupHandlerGetByPoints(Map<String, Integer> answers, Map<String, Integer> points,
                                                   long from, long to) throws Exception {
        for (Locator locator : locators) {
            for (Granularity g2 : Granularity.granularities()) {
                JSONArray data = (JSONArray) httpHandler.GetDataByPoints(
                        locator.getTenantId(),
                        locator.getMetricName(),
                        baseMillis,
                        baseMillis + 86400000,
                        points.get(g2.name())).get("values");
                Assert.assertEquals((int) answers.get(g2.name()), data.size());
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
        final JSONArray values =  (JSONArray) handler.GetDataByResolution(locator.getTenantId(),
                locator.getMetricName(), from, to, resolution).get("values");
        return values.size();
    }

    private void testHttpRequestForPoints() throws Exception {
        testHappyCaseHTTPRequest();
        testBadRequest();
        testBadMethod();
    }

    private void testHappyCaseHTTPRequest() throws Exception {
        HttpGet get = new HttpGet(getMetricsQueryURI());
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
        Assert.assertEquals(501, response.getStatusLine().getStatusCode());
    }

    private void generateRollups(Locator locator, long from, long to, Granularity destGranularity) throws Exception {
        if (destGranularity == Granularity.FULL) {
            throw new Exception("Can't roll up to FULL");
        }

        Map<Long, Rollup> rollups = new HashMap<Long, Rollup>();
        for (Range range : Range.rangesForInterval(destGranularity, from, to)) {
            Rollup rollup = AstyanaxReader.getInstance().readAndCalculate(locator, range, Granularity.FULL);
            rollups.put(range.getStart(), rollup);
        }

        AstyanaxWriter.getInstance().insertRollups(locator, rollups, destGranularity);
    }

    private URI getMetricsQueryURI() throws URISyntaxException {
        URIBuilder builder = new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(queryPort).setPath("/v1.0/" + tenantId + "/experimental/views/metric_data/" + metricName)
                .setParameter("from", String.valueOf(baseMillis))
                .setParameter("to", String.valueOf(baseMillis + 86400000))
                .setParameter("resolution", "full");
        return builder.build();
    }

    private URI getInvalidMetricsQueryURI() throws URISyntaxException {
        URIBuilder builder = new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(queryPort).setPath("/v1.0/" + tenantId + "/experimental/views/metric_data/" + metricName)
                .setParameter("from", String.valueOf(baseMillis))
                .setParameter("resolution", "full");  // Misses parameter 'to'
        return builder.build();
    }

    @AfterClass
    public static void shutdown() {
        vendor.shutdown();
    }
}
