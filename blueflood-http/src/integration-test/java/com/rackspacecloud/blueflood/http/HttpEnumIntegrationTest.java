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

import com.github.tlrx.elasticsearch.test.EsSetup;
import com.rackspacecloud.blueflood.inputs.handlers.HttpEventsIngestionHandler;
import com.rackspacecloud.blueflood.inputs.handlers.HttpMetricsIngestionServer;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.service.*;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.utils.ModuleLoader;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.Mockito.spy;

public class HttpEnumIntegrationTest {

    private static HttpIngestionService httpIngestionService;
    private static HttpQueryService httpQueryService;
    private static HttpClientVendor vendor;
    private static DefaultHttpClient client;
    private static Collection<Integer> manageShards = new HashSet<Integer>();
    private static int httpPortIngest;
    private static int httpPortQuery;
    private static ScheduleContext context;
    private static EventsIO eventsSearchIO;
    private static EsSetup esSetup;

    @BeforeClass
    public static void setUp() throws Exception{
        Configuration.getInstance().init();
        System.setProperty(CoreConfig.DISCOVERY_MODULES.name(), "com.rackspacecloud.blueflood.io.ElasticIO");
        System.setProperty(CoreConfig.ENUMS_DISCOVERY_MODULES.name(), "com.rackspacecloud.blueflood.io.EnumElasticIO");
        System.setProperty(CoreConfig.EVENTS_MODULES.name(), "com.rackspacecloud.blueflood.io.EventElasticSearchIO");

        // setup servers
        httpPortIngest = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_INGESTION_PORT);
        httpPortQuery = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_METRIC_DATA_QUERY_PORT);
        manageShards.add(1); manageShards.add(5); manageShards.add(6);
        context = spy(new ScheduleContext(System.currentTimeMillis(), manageShards));

        // setup elasticsearch
        esSetup = new EsSetup();
        esSetup.execute(EsSetup.deleteAll());
        esSetup.execute(EsSetup.createIndex(ElasticIO.INDEX_NAME_WRITE)
                .withSettings(EsSetup.fromClassPath("index_settings.json"))
                .withMapping("metrics", EsSetup.fromClassPath("metrics_mapping.json")));
        esSetup.execute(EsSetup.createIndex(EnumElasticIO.ENUMS_INDEX_NAME_WRITE)
                .withSettings(EsSetup.fromClassPath("index_settings.json"))
                .withMapping(EnumElasticIO.ENUMS_DOCUMENT_TYPE, EsSetup.fromClassPath("metrics_mapping_enums.json")));
        esSetup.execute(EsSetup.createIndex(EventElasticSearchIO.EVENT_INDEX)
                .withSettings(EsSetup.fromClassPath("index_settings.json"))
                .withMapping("graphite_event", EsSetup.fromClassPath("events_mapping.json")));
        eventsSearchIO = new EventElasticSearchIO(esSetup.client());
        ((ElasticIO) ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.DISCOVERY_MODULES)).setClient(esSetup.client());
        ((EnumElasticIO) ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.ENUMS_DISCOVERY_MODULES)).setClient(esSetup.client());

        // setup ingestion server
        HttpMetricsIngestionServer server = new HttpMetricsIngestionServer(context, new AstyanaxMetricsWriter());
        server.setHttpEventsIngestionHandler(new HttpEventsIngestionHandler(eventsSearchIO));
        httpIngestionService = new HttpIngestionService();
        httpIngestionService.setMetricsIngestionServer(server);
        httpIngestionService.startService(context, new AstyanaxMetricsWriter());

        // setup query server
        httpQueryService = new HttpQueryService();
        httpQueryService.startService();

        vendor = new HttpClientVendor();
        client = vendor.getClient();
    }

    @Test
    public void testMetricIngestionWithEnum() throws Exception {
        // ingest and rollup metrics with enum values and verify CF points and elastic search indexes
        final String tenant_id = "333333";
        final String metric_name = "enum_metric_test";
        Set<Locator> locators = new HashSet<Locator>();
        locators.add(Locator.createLocatorFromPathComponents(tenant_id, metric_name));

        // post enum metric for ingestion and verify
        HttpResponse response = postAggregatedMetric(tenant_id, "src/test/resources/sample_enums_payload.json");
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

    private HttpResponse postAggregatedMetric(String tenantId, String payloadFilePath) throws URISyntaxException, IOException {
        // get payload
        StringBuilder sb = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(payloadFilePath)));
        String curLine = reader.readLine();
        while (curLine != null) {
            sb = sb.append(curLine);
            curLine = reader.readLine();
        }
        String json = sb.toString();

        // build url to aggregated ingestion endpoint
        URIBuilder builder = getMetricsURIBuilder()
                .setPath(String.format("/v2.0/%s/ingest/aggregated", tenantId));
        HttpPost post = new HttpPost(builder.build());
        HttpEntity entity = new StringEntity(json, ContentType.APPLICATION_JSON);
        post.setEntity(entity);

        // post and return response
        return client.execute(post);
    }

    private HttpResponse queryMetricIncludeEnum(String tenantId, String metricName) throws URISyntaxException, IOException {
        URIBuilder query_builder = getMetricDataQueryURIBuilder()
                .setPath(String.format("/v2.0/%s/metrics/search", tenantId));
        query_builder.setParameter("query", metricName);
        query_builder.setParameter("include_enum_values", "true");
        HttpGet query_get = new HttpGet(query_builder.build());
        return client.execute(query_get);
    }

    private URIBuilder getMetricsURIBuilder() throws URISyntaxException {
        return new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(httpPortIngest).setPath("/v2.0/acTEST/ingest");
    }

    private URIBuilder getMetricDataQueryURIBuilder() throws URISyntaxException {
        return new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(httpPortQuery).setPath("/v2.0/acTEST/metrics/search")
                .setParameter("query", "call*");
    }

    @AfterClass
    public static void shutdown() {
        Configuration.getInstance().setProperty(CoreConfig.DISCOVERY_MODULES.name(), "");
        Configuration.getInstance().setProperty(CoreConfig.ENUMS_DISCOVERY_MODULES.name(), "");
        Configuration.getInstance().setProperty(CoreConfig.EVENTS_MODULES.name(), "");
        Configuration.getInstance().setProperty(CoreConfig.ENUM_UNIQUE_VALUES_THRESHOLD.name(), "100");

        if (esSetup != null) {
            esSetup.terminate();
        }

        if (vendor != null) {
            vendor.shutdown();
        }

        if (httpQueryService != null) {
            httpQueryService.stopService();
        }

        if (httpIngestionService != null) {
            httpIngestionService.shutdownService();
        }
    }
}
