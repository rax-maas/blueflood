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
import com.rackspacecloud.blueflood.utils.ModuleLoader;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashSet;

import static org.mockito.Mockito.spy;

public class HttpIntegrationTestBase {
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

    public final String postAggregatedPath = "/v2.0/%s/ingest/aggregated";
    public final String postAggregatedMultiPath = "/v2.0/%s/ingest/aggregated/multi";

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


    @AfterClass
    public static void shutdown() {
        Configuration.getInstance().setProperty(CoreConfig.DISCOVERY_MODULES.name(), "");
        Configuration.getInstance().setProperty(CoreConfig.ENUMS_DISCOVERY_MODULES.name(), "");
        Configuration.getInstance().setProperty(CoreConfig.EVENTS_MODULES.name(), "");

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

    public HttpResponse postMetric(String tenantId, String urlPath, String payloadFilePath) throws URISyntaxException, IOException {
        // post metric to ingestion server for a tenantId
        // urlPath is path for url ingestion after the hostname
        // payloadFilepath is location of the payload for the POST content entity

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
                .setPath(String.format(urlPath, tenantId));
        HttpPost post = new HttpPost(builder.build());
        HttpEntity entity = new StringEntity(json, ContentType.APPLICATION_JSON);
        post.setEntity(entity);

        // post and return response
        return client.execute(post);
    }

    public HttpResponse queryMetricIncludeEnum(String tenantId, String metricName) throws URISyntaxException, IOException {
        URIBuilder query_builder = getMetricDataQueryURIBuilder()
                .setPath(String.format("/v2.0/%s/metrics/search", tenantId));
        query_builder.setParameter("query", metricName);
        query_builder.setParameter("include_enum_values", "true");
        HttpGet query_get = new HttpGet(query_builder.build());
        return client.execute(query_get);
    }

    public HttpResponse querySingleplot(String tenantId, String metricName, long fromTime, long toTime, String points, String resolution, String select)
            throws URISyntaxException, IOException {
        URIBuilder query_builder = getRollupsQueryURIBuilder()
                .setPath(String.format("/v2.0/%s/views/%s", tenantId, metricName));
        query_builder.setParameter("from", Long.toString(fromTime));
        query_builder.setParameter("to", Long.toString(toTime));

        if (!points.isEmpty()) {
            query_builder.setParameter("points", points);
        }

        if (!resolution.isEmpty()) {
            query_builder.setParameter("resolution", resolution);
        }

        if (!select.isEmpty()) {
            query_builder.setParameter("select", select);
        }

        HttpGet query_get = new HttpGet(query_builder.build());
        return client.execute(query_get);
    }

    public HttpResponse queryMultiplot(String tenantId, long fromTime, long toTime, String points, String resolution, String select, String metricNames)
            throws URISyntaxException, IOException {
        URIBuilder query_builder = getRollupsQueryURIBuilder()
                .setPath(String.format("/v2.0/%s/views", tenantId));
        query_builder.setParameter("from", Long.toString(fromTime));
        query_builder.setParameter("to", Long.toString(toTime));

        if (!points.isEmpty()) {
            query_builder.setParameter("points", points);
        }

        if (!resolution.isEmpty()) {
            query_builder.setParameter("resolution", resolution);
        }

        if (!select.isEmpty()) {
            query_builder.setParameter("select", select);
        }

        HttpPost query_post = new HttpPost(query_builder.build());
        HttpEntity entity = new StringEntity(metricNames);
        query_post.setEntity(entity);

        return client.execute(query_post);
    }

    public URIBuilder getMetricsURIBuilder() throws URISyntaxException {
        return new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(httpPortIngest).setPath("/v2.0/tenantId/ingest");
    }

    public URIBuilder getMetricDataQueryURIBuilder() throws URISyntaxException {
        return new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(httpPortQuery).setPath("/v2.0/tenantId/metrics/search")
                .setParameter("query", "call*");
    }

    public URIBuilder getRollupsQueryURIBuilder() throws URISyntaxException {
        return new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(httpPortQuery).setPath("/v2.0/tenantId/views")
                .setParameter("from", "100000000")
                .setParameter("to", "200000000");
    }

}
