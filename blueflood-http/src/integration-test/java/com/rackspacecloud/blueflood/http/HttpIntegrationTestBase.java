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
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxMetricsWriter;
import com.rackspacecloud.blueflood.service.*;
import com.rackspacecloud.blueflood.types.Event;
import com.rackspacecloud.blueflood.utils.ModuleLoader;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.*;

import java.io.*;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Random;

import static org.mockito.Mockito.spy;
import static com.rackspacecloud.blueflood.TestUtils.*;

public class HttpIntegrationTestBase {

    public static final long TIME_DIFF_MS = 20000;

    protected static HttpClient client;
    protected static ScheduleContext context;
    protected static EventsIO eventsSearchIO;
    protected static EsSetup esSetup;

    private static HttpIngestionService httpIngestionService;
    private static HttpQueryService httpQueryService;
    private static HttpClientVendor vendor;
    private static Collection<Integer> manageShards = new HashSet<Integer>();
    private static int httpPortIngest;
    private static int httpPortQuery;

    public final String postPath = "/v2.0/%s/ingest";
    public final String postEventsPath = "/v2.0/%s/events";
    public final String postMultiPath = "/v2.0/%s/ingest/multi";
    public final String postAggregatedPath = "/v2.0/%s/ingest/aggregated";
    public final String postAggregatedMultiPath = "/v2.0/%s/ingest/aggregated/multi";
    private Random random = new Random( System.currentTimeMillis() );

    @BeforeClass
    public static void setUp() throws Exception{
        Configuration.getInstance().init();
        // This is to help with Travis, which intermittently fail the following tests due
        // to getting TimeoutException. This is done here because it needs to be before
        // RollupHandler is instantiated.
        Configuration.getInstance().setProperty(CoreConfig.ROLLUP_ON_READ_TIMEOUT_IN_SECONDS, "20");
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
        esSetup.execute(EsSetup.createIndex(ElasticIO.ELASTICSEARCH_INDEX_NAME_WRITE)
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
        System.clearProperty(CoreConfig.DISCOVERY_MODULES.name());
        System.clearProperty(CoreConfig.ENUMS_DISCOVERY_MODULES.name());
        System.clearProperty(CoreConfig.EVENTS_MODULES.name());

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

    public HttpResponse postGenMetric( String tenantId, String postfix, String url ) throws Exception {

        return httpPost( tenantId, url, generateJSONMetricsData( postfix ) );
    }

    public HttpResponse postGenMetric( String tenantId, String postfix, String url, long time ) throws Exception {

        return httpPost( tenantId, url, generateJSONMetricsData( postfix, time ) );
    }

    public HttpResponse postMetric(String tenantId, String urlPath, String payloadFilePath, String postfix) throws URISyntaxException, IOException {
        // post metric to ingestion server for a tenantId
        // urlPath is path for url ingestion after the hostname
        // payloadFilepath is location of the payload for the POST content entity

        String json = getJsonFromFile( new InputStreamReader( getClass().getClassLoader().getResourceAsStream( payloadFilePath ) ),
                postfix );
        return httpPost( tenantId, urlPath, json );
    }

    public HttpResponse postMetric(String tenantId, String urlPath, String payloadFilePath, long timestamp, String postfix ) throws URISyntaxException, IOException {
        // post metric to ingestion server for a tenantId
        // urlPath is path for url ingestion after the hostname
        // payloadFilepath is location of the payload for the POST content entity

        String json = getJsonFromFile( new InputStreamReader( getClass().getClassLoader().getResourceAsStream( payloadFilePath ) ),
                timestamp, postfix );
        return httpPost( tenantId, urlPath, json );
    }


    public HttpResponse postEvent( String tenantId, String requestBody ) throws Exception {


        HttpPost post = getHttpPost( tenantId, postEventsPath, requestBody );

        post.setHeader( Event.FieldLabels.tenantId.name(), tenantId);
        HttpResponse response = client.execute(post);
        return response;
    }

    public HttpResponse httpPost( String tenantId, String urlPath, String json ) throws URISyntaxException, IOException {

        HttpPost post = getHttpPost( tenantId, urlPath, json );

        return client.execute(post);
    }

    private HttpPost getHttpPost( String tenantId, String urlPath, String json ) throws URISyntaxException {
        // build url to aggregated ingestion endpoint
        URIBuilder builder = getMetricsURIBuilder()
                .setPath(String.format(urlPath, tenantId));
        HttpPost post = new HttpPost(builder.build());
        HttpEntity entity = new StringEntity(json, ContentType.APPLICATION_JSON);
        post.setEntity(entity);
        return post;
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

    protected String[] getBodyArray( HttpResponse response ) throws IOException {

        try {
            StringWriter sw = new StringWriter();
            IOUtils.copy( response.getEntity().getContent(), sw );
            return sw.toString().split( System.lineSeparator() );
        }
        finally {
            IOUtils.closeQuietly( response.getEntity().getContent() );
        }
    }

    protected String getPostfix() {
        return "."  + random.nextInt( 99999 );
    }


    protected String createTestEvent( int batchSize ) throws Exception {

        return createTestEvent( batchSize, System.currentTimeMillis() );
    }

    protected String createTestEvent(int batchSize, long timestamp) throws Exception {
        StringBuilder events = new StringBuilder();
        for (int i=0; i<batchSize; i++) {
            Event event = new Event();
            event.setWhat("deployment "+i);
            event.setWhen( timestamp );
            event.setData("deploying prod "+i);
            event.setTags("deployment "+i);
            events.append(new ObjectMapper().writeValueAsString(event));
        }
        return events.toString();
    }
}
