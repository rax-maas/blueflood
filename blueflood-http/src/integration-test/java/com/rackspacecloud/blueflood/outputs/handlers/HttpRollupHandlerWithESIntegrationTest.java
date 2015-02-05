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

import com.github.tlrx.elasticsearch.test.EsSetup;
import com.rackspacecloud.blueflood.http.HttpClientVendor;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.service.HttpConfig;
import com.rackspacecloud.blueflood.service.HttpQueryService;
import com.rackspacecloud.blueflood.types.*;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.elasticsearch.client.Client;
import org.junit.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class HttpRollupHandlerWithESIntegrationTest extends IntegrationTestBase {
    private final long baseMillis = 1335820166000L;
    private final String tenantId = "ac" + IntegrationTestBase.randString(8);
    private final String metricName = "met_" + IntegrationTestBase.randString(8);
    private final Locator locator = Locator.createLocatorFromPathComponents(tenantId, metricName);
    private static int queryPort = 20000;
    private final Map<Locator, Map<Granularity, Integer>> locatorToPoints = new HashMap<Locator, Map<Granularity,Integer>>();
    private HttpRollupsQueryHandler httpHandler;
    private ElasticIO elasticIO;
    private EsSetup esSetup;
    private static HttpQueryService httpQueryService;
    private static HttpClientVendor vendor;
    private static DefaultHttpClient client;

    @BeforeClass
    public static void setUpHttp() {
        Configuration.getInstance().setProperty(CoreConfig.DISCOVERY_MODULES.name(),
                "com.rackspacecloud.blueflood.outputs.handlers.HttpRollupHandlerWithESIntegrationTest.ElasticIOTest");
        Configuration.getInstance().setProperty(HttpConfig.HTTP_METRIC_DATA_QUERY_PORT.name(), "20001");
        Configuration.getInstance().setProperty(CoreConfig.USE_ES_FOR_UNITS.name(), "true");
        queryPort = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_METRIC_DATA_QUERY_PORT);
        httpQueryService = new HttpQueryService();
        httpQueryService.startService();
        vendor = new HttpClientVendor();
        client = vendor.getClient();
    }

    @Before
    public void setup() throws Exception {
        super.setUp();
        AstyanaxWriter writer = AstyanaxWriter.getInstance();

        esSetup = new EsSetup();
        esSetup.execute(EsSetup.deleteAll());
        esSetup.execute(EsSetup.createIndex(ElasticIO.INDEX_NAME).withMapping("metrics", EsSetup.fromClassPath("metrics_mapping.json")));
        elasticIO = new ElasticIO(esSetup.client());

        final List<Metric> metrics = new ArrayList<Metric>();
        for (int i = 0; i < 1440; i++) {
            final long curMillis = baseMillis + i * 60000;
            final Metric metric = getRandomIntMetric(locator, curMillis);
            metrics.add(metric);
        }

        elasticIO.insertDiscovery(new ArrayList<IMetric>(metrics));
        esSetup.client().admin().indices().prepareRefresh().execute().actionGet();

        writer.insertFull(metrics);

        httpHandler = new HttpRollupsQueryHandler();

        // generate every level of rollup for the raw data
        Granularity g = Granularity.FULL;
        while (g != Granularity.MIN_1440) {
            g = g.coarser();
            generateRollups(locator, baseMillis, baseMillis + 86400000, g);
        }

        final Map<Granularity, Integer> answerForNumericMetric = new HashMap<Granularity, Integer>();
        answerForNumericMetric.put(Granularity.FULL, 1440);
        answerForNumericMetric.put(Granularity.MIN_5, 289);
        answerForNumericMetric.put(Granularity.MIN_20, 73);
        answerForNumericMetric.put(Granularity.MIN_60, 25);
        answerForNumericMetric.put(Granularity.MIN_240, 7);
        answerForNumericMetric.put(Granularity.MIN_1440, 2);

        locatorToPoints.put(locator, answerForNumericMetric);
    }

    @After
    public void tearDown() {
        esSetup.terminate();
    }

    @Test
    public void testMetricDataFetching() throws Exception {
        final Map<Granularity, Integer> points = new HashMap<Granularity, Integer>();
        points.put(Granularity.FULL, 1600);
        points.put(Granularity.MIN_5, 287);
        points.put(Granularity.MIN_20, 71);
        points.put(Granularity.MIN_60, 23);
        points.put(Granularity.MIN_240, 5);
        points.put(Granularity.MIN_1440, 1);
        for (Granularity g2 : Granularity.granularities()) {
            MetricData data = httpHandler.GetDataByPoints(
                    locator.getTenantId(),
                    locator.getMetricName(),
                    baseMillis,
                    baseMillis + 86400000,
                    points.get(g2));
            Assert.assertEquals((int) locatorToPoints.get(locator).get(g2), data.getData().getPoints().size());
            Assert.assertEquals(locatorToUnitMap.get(locator), data.getUnit());
        }
        Assert.assertEquals(locatorToUnitMap.get(locator), elasticIO.search(locator.getTenantId(), locator.getMetricName()).get(0).getUnit());
        /* TODO: Fix the test to check for simple http return status codes
        HttpGet get = new HttpGet(getMetricsQueryURI());
        HttpResponse response = client.execute(get);
        System.out.println(response.getEntity().getContent());
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        */
    }

    private URI getMetricsQueryURI() throws URISyntaxException {
        URIBuilder builder = new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(queryPort).setPath("/v2.0/" + tenantId + "/views/" + metricName)
                .setParameter("from", String.valueOf(baseMillis))
                .setParameter("to", String.valueOf(baseMillis + 86400000))
                .setParameter("resolution", "full");
        return builder.build();
    }

    class ElasticIOTest extends ElasticIO {
        Client esClient;
        public ElasticIOTest() {
            super();
            EsSetup esSetup = new EsSetup();
            //esSetup.execute(EsSetup.deleteAll());
            esSetup.execute(EsSetup.createIndex(com.rackspacecloud.blueflood.io.ElasticIO.INDEX_NAME).withMapping("metrics", EsSetup.fromClassPath("metrics_mapping.json")));
            esClient = esSetup.client();
            setClient(esClient);
        }
        public Client getESClient() {
            return esClient;
        }
    }
}
