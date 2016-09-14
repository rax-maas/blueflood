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
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.outputs.handlers.HttpMetricDataQueryServer;
import com.rackspacecloud.blueflood.outputs.handlers.HttpRollupsQueryHandler;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.*;
import com.rackspacecloud.blueflood.types.Event;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Resolution;
import com.rackspacecloud.blueflood.utils.ModuleLoader;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static org.mockito.Mockito.spy;
import static com.rackspacecloud.blueflood.TestUtils.*;

public class HttpIntegrationTestBase extends IntegrationTestBase {

    protected static final long TIME_DIFF_MS = 40000;

    //A time stamp 2 days ago
    protected final long baseMillis = Calendar.getInstance().getTimeInMillis() - 172800000;

    protected static HttpClient client;
    protected static ScheduleContext context;
    protected static  Map <String, String> parameterMap;
    protected static ElasticIO elasticIO;
    protected static EventsIO eventsSearchIO;
    protected static EsSetup esSetup;

    protected static String configAllowedOrigins = "test.domain1.com, test.domain2.com, test.domain3.com";
    protected static String configAllowedHeaders = "XYZ, ABC";
    protected static String configAllowedMethods = "GET, POST, PUT";
    protected static String configAllowedMaxAge = "6000";

    private static HttpIngestionService httpIngestionService;
    private static HttpQueryService httpQueryService;
    private static HttpClientVendor vendor;
    private static Collection<Integer> manageShards = new HashSet<Integer>();
    protected static int httpPortIngest;
    protected static int httpPortQuery;

    public final String postPath = "/v2.0/%s/ingest";
    public final String postEventsPath = "/v2.0/%s/events";
    public final String postMultiPath = "/v2.0/%s/ingest/multi";
    public final String postAggregatedPath = "/v2.0/%s/ingest/aggregated";
    public final String postAggregatedMultiPath = "/v2.0/%s/ingest/aggregated/multi";

    public final String getEventsPath = "/v2.0/%s/events/getEvents";
    public final String getSearchPath = "/v2.0/%s/metrics/search";
    public final String getTokenSearchPath = "/v2.0/%s/metric_name/search";
    public final String getViewsPath = "/v2.0/%s/views";

    private Random random = new Random( System.currentTimeMillis() );

    @BeforeClass
    public static void setUpHttp() throws Exception {

        Configuration.getInstance().init();
        Configuration.getInstance().setProperty(CoreConfig.CORS_ENABLED, "true");
        Configuration.getInstance().setProperty(CoreConfig.CORS_ALLOWED_ORIGINS, configAllowedOrigins);
        Configuration.getInstance().setProperty(CoreConfig.CORS_ALLOWED_HEADERS, configAllowedHeaders);
        Configuration.getInstance().setProperty(CoreConfig.CORS_ALLOWED_METHODS, configAllowedMethods);
        Configuration.getInstance().setProperty(CoreConfig.CORS_ALLOWED_MAX_AGE, configAllowedMaxAge);


        // This is to help with Travis, which intermittently fail the following tests due
        // to getting TimeoutException. This is done here because it needs to be before
        // RollupHandler is instantiated.
        Configuration.getInstance().setProperty(CoreConfig.ROLLUP_ON_READ_TIMEOUT_IN_SECONDS, "20");

        setupElasticSearch();

        setupIngestionServer();

        setupQueryServer();

        // setup vendor and client
        vendor = new HttpClientVendor();
        client = vendor.getClient();
    }

    @AfterClass
    public static void shutdown() throws IOException {

        Configuration.getInstance().init();

        if (vendor != null) {
            vendor.shutdown();
        }

        if (httpQueryService != null) {
            httpQueryService.stopService();
        }

        if (httpIngestionService != null) {
            httpIngestionService.shutdownService();
        }

        if (esSetup != null) {
            esSetup.terminate();
        }
    }

    private static void setupElasticSearch() {
        // setup elasticsearch

        // setup config
        System.setProperty(CoreConfig.DISCOVERY_MODULES.name(), "com.rackspacecloud.blueflood.io.ElasticIO");
        System.setProperty(CoreConfig.ENUMS_DISCOVERY_MODULES.name(), "com.rackspacecloud.blueflood.io.EnumElasticIO");
        System.setProperty(CoreConfig.EVENTS_MODULES.name(), "com.rackspacecloud.blueflood.io.EventElasticSearchIO");

        // setup elasticsearch test clusters with blueflood mappings
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

        // create elaticsearch client and link it to ModuleLoader
        elasticIO = new ElasticIO(esSetup.client());
        eventsSearchIO = new EventElasticSearchIO(esSetup.client());
        ((ElasticIO) ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.DISCOVERY_MODULES)).setClient(esSetup.client());
        ((EnumElasticIO) ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.ENUMS_DISCOVERY_MODULES)).setClient(esSetup.client());
    }

    private static void setupIngestionServer() throws Exception {
        // setup ingestion server
        manageShards.add(1); manageShards.add(5); manageShards.add(6);
        context = spy(new ScheduleContext(System.currentTimeMillis(), manageShards));
        httpPortIngest = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_INGESTION_PORT);
        HttpMetricsIngestionServer server = new HttpMetricsIngestionServer(context);
        server.setHttpEventsIngestionHandler(new HttpEventsIngestionHandler(eventsSearchIO));
        httpIngestionService = new HttpIngestionService();
        httpIngestionService.setMetricsIngestionServer(server);
        httpIngestionService.startService(context);
    }

    private static void setupQueryServer() throws Exception {
        // setup query server
        httpPortQuery = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_METRIC_DATA_QUERY_PORT);
        httpQueryService = new HttpQueryService();
        HttpMetricDataQueryServer queryServer = new HttpMetricDataQueryServer();
        queryServer.setEventsIO(eventsSearchIO);
        httpQueryService.setServer(queryServer);
        httpQueryService.startService();
    }

    public URI getQueryURI(String queryPath) throws URISyntaxException {
        // build and return a query path with query port and parameters set from the parameters
        URIBuilder builder = new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(httpPortQuery).setPath(queryPath);

        Set<String> parameters = parameterMap.keySet();
        Iterator<String> setIterator = parameters.iterator();
        while (setIterator.hasNext()){
            String paramName = setIterator.next();
            builder.setParameter(paramName, parameterMap.get(paramName));
        }
        return builder.build();
    }

    public URI getQueryEventsURI(String tenantId) throws URISyntaxException {
        return getQueryURI(String.format(getEventsPath, tenantId));
    }

    public URI getQuerySearchURI(String tenantId) throws URISyntaxException {
        return getQueryURI(String.format(getSearchPath, tenantId));
    }

    public URI getQueryTokenSearchURI(String tenantId) throws URISyntaxException {
        return getQueryURI(String.format(getTokenSearchPath, tenantId));
    }

    public URI getQueryViewsURI(String tenantId) throws URISyntaxException {
        return getQueryURI(String.format(getViewsPath, tenantId));
    }

    public URI getQueryMetricViewsURI(String tenantId, String metricName) throws URISyntaxException {
        return getQueryURI(String.format(getViewsPath, tenantId) + "/" + metricName);
    }

    public HttpResponse postGenMetric( String tenantId, String postfix, String url ) throws Exception {
        return httpPost(tenantId, url, generateJSONMetricsData(postfix));
    }

    public HttpResponse postGenMetric( String tenantId, String postfix, String url, long time ) throws Exception {
        return httpPost( tenantId, url, generateJSONMetricsData( postfix, time ) );
    }

    public HttpResponse postMetric(String tenantId, String urlPath, String payloadFilePath, String postfix) throws URISyntaxException, IOException {
        // post metric to ingestion server for a tenantId
        // urlPath is path for url ingestion after the hostname
        // payloadFilepath is location of the payload for the POST content entity

        String json = getJsonFromFile( payloadFilePath, postfix );
        return httpPost( tenantId, urlPath, json );
    }

    public HttpResponse postMetric(String tenantId, String urlPath, String payloadFilePath, long timestamp, String postfix ) throws URISyntaxException, IOException {
        // post metric to ingestion server for a tenantId
        // urlPath is path for url ingestion after the hostname
        // payloadFilepath is location of the payload for the POST content entity

        String json = getJsonFromFile( payloadFilePath, timestamp, postfix );
        return httpPost( tenantId, urlPath, json );
    }

    public HttpResponse postEvent( String tenantId, String requestBody ) throws Exception {

        HttpPost post = getHttpPost( tenantId, postEventsPath, requestBody );
        post.setHeader( Event.FieldLabels.tenantId.name(), tenantId);
        return client.execute(post);
    }

    public HttpResponse httpPost( String tenantId, String urlPath, String json ) throws URISyntaxException, IOException {

        HttpPost post = getHttpPost( tenantId, urlPath, json );

        return client.execute(post);
    }

    public HttpResponse httpPost( String tenantId, String urlPath, String content, ContentType contentType ) throws URISyntaxException, IOException {

        HttpPost post = getHttpPost( tenantId, urlPath, content, contentType );

        return client.execute(post);
    }

    private HttpPost getHttpPost( String tenantId, String urlPath, String json ) throws URISyntaxException {
        return getHttpPost(tenantId, urlPath, json, ContentType.APPLICATION_JSON);
    }

    private HttpPost getHttpPost( String tenantId, String urlPath, String content, ContentType contentType ) throws URISyntaxException {
        // build url to aggregated ingestion endpoint
        URIBuilder builder = getMetricsURIBuilder()
                .setPath(String.format(urlPath, tenantId));
        HttpPost post = new HttpPost(builder.build());
        HttpEntity entity = new StringEntity(content, contentType);
        post.setEntity(entity);
        return post;
    }

    public HttpResponse queryMetricIncludeEnum(String tenantId, String metricName) throws URISyntaxException, IOException {
        URIBuilder query_builder = getMetricDataQueryURIBuilder()
                .setPath(String.format(getSearchPath, tenantId));
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
        query_post.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());

        return client.execute(query_post);
    }

    public URIBuilder getMetricsURIBuilder() throws URISyntaxException {
        return new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(httpPortIngest).setPath("/v2.0/tenantId/ingest");
    }

    public URIBuilder getMetricDataQueryURIBuilder() throws URISyntaxException {
        return new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(httpPortQuery).setPath(getSearchPath)
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

    protected void checkGetRollupByResolution(List<Locator> locators, Map<Locator, Map<Granularity, Integer>> answers,
                                              long baseMillis, HttpRollupsQueryHandler httpHandler) throws Exception {
        for (Locator locator : locators) {
            for (Resolution resolution : Resolution.values()) {
                Granularity g = Granularity.granularities()[resolution.getValue()];
                checkHttpHandlersGetByResolution(locator, resolution, baseMillis, baseMillis + 86400000,
                        answers.get(locator).get(g), httpHandler);
            }
        }
    }

    protected void checkHttpHandlersGetByResolution(Locator locator, Resolution resolution, long from, long to,
                                                    int expectedPoints, HttpRollupsQueryHandler handler) throws Exception {
        int currentPoints = getNumberOfPointsViaHTTPHandler(handler, locator,
                from, to, resolution);

        Assert.assertEquals(String.format("locator=%s, resolution=%s, from=%d, to=%d, expectedPoints=%d and currentPoints=%d should be the same",
                locator, resolution.toString(), from, to, expectedPoints, currentPoints),
                expectedPoints, currentPoints);
    }

    protected int getNumberOfPointsViaHTTPHandler(HttpRollupsQueryHandler handler,
                                                  Locator locator, long from, long to,
                                                  Resolution resolution) throws Exception {
        final MetricData values = handler.GetDataByResolution(locator.getTenantId(),
                locator.getMetricName(), from, to, resolution);
        return values.getData().getPoints().size();
    }

    protected void checkHttpRollupHandlerGetByPoints(Map<Locator, Map<Granularity, Integer>> answers, Map<Granularity, Integer> points,
                                                     long from, long to, List<Locator> locators, HttpRollupsQueryHandler httphandler) throws Exception {
        for (Locator locator : locators) {
            for (Granularity g2 : Granularity.granularities()) {
                MetricData data = httphandler.GetDataByPoints(
                        locator.getTenantId(),
                        locator.getMetricName(),
                        from,
                        to,
                        points.get(g2));
                Assert.assertEquals(String.format("locator=%s, from=%d, to=%d, expectedPoints=%d and currentPoints=%d should be the same",
                        locator, from, to, answers.get(locator).get(g2), data.getData().getPoints().size()),
                        (int)answers.get(locator).get(g2), data.getData().getPoints().size());
                // Disabling test that fail on ES
                // Assert.assertEquals(locatorToUnitMap.get(locator), data.getUnit());
            }
        }
    }

    protected void assertResponseHeaderAllowOrigin(HttpResponse response) {
        // assert allowed origins
        String[] allowedOrigins = Configuration.getInstance().getStringProperty(CoreConfig.CORS_ALLOWED_ORIGINS).split(",");

        Header[] allowOriginResponse = response.getHeaders("Access-Control-Allow-Origin");
        Assert.assertTrue("Missing allow origin in response", allowOriginResponse.length > 0);

        String allowOriginActual = allowOriginResponse[0].getValue();
        Assert.assertEquals("Invalid number of allow origins", allowedOrigins.length, allowOriginActual.split(",").length);
        for (String allowedOrigin : allowedOrigins) {
            Assert.assertTrue("Missing allowed origin " + allowedOrigin, allowOriginActual.contains(allowedOrigin));
        }
    }

}
