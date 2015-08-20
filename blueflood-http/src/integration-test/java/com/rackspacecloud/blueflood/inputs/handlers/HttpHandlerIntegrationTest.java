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

package com.rackspacecloud.blueflood.inputs.handlers;

import com.github.tlrx.elasticsearch.test.EsSetup;
import com.rackspacecloud.blueflood.http.HttpClientVendor;
import com.rackspacecloud.blueflood.inputs.formats.JSONMetricsContainerTest;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.*;
import com.rackspacecloud.blueflood.types.*;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.zip.GZIPOutputStream;
import static org.mockito.Mockito.*;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class HttpHandlerIntegrationTest {
    private static HttpIngestionService httpIngestionService;
    private static HttpClientVendor vendor;
    private static DefaultHttpClient client;
    private static Collection<Integer> manageShards = new HashSet<Integer>();
    private static int httpPort;
    private static ScheduleContext context;
    private static EventsIO eventsSearchIO;
    private static EsSetup esSetup;
    //A time stamp 2 days ago
    private final long baseMillis = Calendar.getInstance().getTimeInMillis() - 172800000;

    @BeforeClass
    public static void setUp() throws Exception{
        System.setProperty(CoreConfig.EVENTS_MODULES.name(), "com.rackspacecloud.blueflood.io.EventElasticSearchIO");
        Configuration.getInstance().init();
        httpPort = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_INGESTION_PORT);
        manageShards.add(1); manageShards.add(5); manageShards.add(6);
        context = spy(new ScheduleContext(System.currentTimeMillis(), manageShards));

        esSetup = new EsSetup();
        esSetup.execute(EsSetup.deleteAll());
        esSetup.execute(EsSetup.createIndex(EventElasticSearchIO.EVENT_INDEX)
                .withSettings(EsSetup.fromClassPath("index_settings.json"))
                .withMapping("metrics", EsSetup.fromClassPath("events_mapping.json")));
        eventsSearchIO = new EventElasticSearchIO(esSetup.client());
        HttpMetricsIngestionServer server = new HttpMetricsIngestionServer(context, new AstyanaxMetricsWriter());
        server.setHttpEventsIngestionHandler(new HttpEventsIngestionHandler(eventsSearchIO));

        httpIngestionService = new HttpIngestionService();
        httpIngestionService.setMetricsIngestionServer(server);
        httpIngestionService.startService(context, new AstyanaxMetricsWriter());

        vendor = new HttpClientVendor();
        client = vendor.getClient();
    }

    @Test
    public void testHttpIngestionHappyCase() throws Exception {
        HttpPost post = new HttpPost(getMetricsURI());
        HttpEntity entity = new StringEntity(JSONMetricsContainerTest.generateJSONMetricsData(),
                ContentType.APPLICATION_JSON);
        post.setEntity(entity);
        HttpResponse response = client.execute(post);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        verify(context, atLeastOnce()).update(anyLong(), anyInt());
        // assert that the update method on the ScheduleContext object was called and completed successfully
        // Now read the metrics back from dcass and check (relies on generareJSONMetricsData from JSONMetricsContainerTest)
        final Locator locator = Locator.createLocatorFromPathComponents("acTEST", "mzord.duration");
        Points<SimpleNumber> points = AstyanaxReader.getInstance().getDataToRoll(SimpleNumber.class,
                locator, new Range(1234567878, 1234567900), CassandraModel.getColumnFamily(BasicRollup.class, Granularity.FULL));
        Assert.assertEquals(1, points.getPoints().size());
        EntityUtils.consume(response.getEntity()); // Releases connection apparently
    }

    @Test
    public void testHttpAnnotationsIngestionHappyCase() throws Exception {
        final int batchSize = 1;
        final String tenant_id = "333333";
        String event = createTestEvent(batchSize);
        postEvent(event, tenant_id);

        //Sleep for a while
        Thread.sleep(1200);
        Map<String, List<String>> query = new HashMap<String, List<String>>();
        query.put(Event.tagsParameterName, Arrays.asList("deployment"));
        List<Map<String, Object>> results = eventsSearchIO.search(tenant_id, query);
        Assert.assertEquals(batchSize, results.size());

        query = new HashMap<String, List<String>>();
        query.put(Event.fromParameterName, Arrays.asList(String.valueOf(baseMillis - 86400000)));
        query.put(Event.untilParameterName, Arrays.asList(String.valueOf(baseMillis + (86400000*3))));
        results = eventsSearchIO.search(tenant_id, query);
        Assert.assertEquals(batchSize, results.size());
    }

    @Test
    public void testHttpAnnotationsIngestionMultiEvents() throws Exception {
        final int batchSize = 5;
        final String tenant_id = "333444";
        String event = createTestEvent(batchSize);
        postEvent(event, tenant_id);

        //Sleep for a while
        Thread.sleep(1200);
        Map<String, List<String>> query = new HashMap<String, List<String>>();
        query.put(Event.tagsParameterName, Arrays.asList("deployment"));
        List<Map<String, Object>> results = eventsSearchIO.search(tenant_id, query);
        Assert.assertFalse(batchSize == results.size()); //Only saving the first event of the batch, so the result size will be 1.
        Assert.assertTrue(results.size() == 1);

        query = new HashMap<String, List<String>>();
        query.put(Event.fromParameterName, Arrays.asList(String.valueOf(baseMillis - 86400000)));
        query.put(Event.untilParameterName, Arrays.asList(String.valueOf(baseMillis + (86400000*3))));
        results = eventsSearchIO.search(tenant_id, query);
        Assert.assertFalse(batchSize == results.size());
        Assert.assertTrue(results.size() == 1);
    }

    @Test
    public void testHttpAnnotationsIngestionDuplicateEvents() throws Exception {
        int batchSize = 5; // To create duplicate events
        String tenant_id = "444444";

        createAndInsertTestEvents(tenant_id, batchSize);
        esSetup.client().admin().indices().prepareRefresh().execute().actionGet();

        Map<String, List<String>> query = new HashMap<String, List<String>>();
        query.put(Event.tagsParameterName, Arrays.asList("deployment"));

        List<Map<String, Object>> results = eventsSearchIO.search(tenant_id, query);
        Assert.assertEquals(batchSize, results.size());

        query = new HashMap<String, List<String>>();
        query.put(Event.fromParameterName, Arrays.asList(String.valueOf(baseMillis - 86400000)));
        query.put(Event.untilParameterName, Arrays.asList(String.valueOf(baseMillis + (86400000*3))));

        results = eventsSearchIO.search(tenant_id, query);
        Assert.assertEquals(batchSize, results.size());
    }

    @Test
    public void testIngestingInvalidJAnnotationsJSON() throws Exception {
        URIBuilder builder = new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(httpPort).setPath("/v2.0/456854/events");
        HttpPost post = new HttpPost(builder.build());
        String requestBody = //Invalid JSON with single inverted commas instead of double.
                "{'when':346550008," +
                        "'what':'Dummy Event'," +
                        "'data':'Dummy Data'," +
                        "'tags':'deployment'}";

        HttpEntity entity = new StringEntity(requestBody,
                ContentType.APPLICATION_JSON);
        post.setEntity(entity);
        post.setHeader(Event.FieldLabels.tenantId.name(), "456854");
        HttpResponse response = client.execute(post);
        String responseString = EntityUtils.toString(response.getEntity());
        Assert.assertEquals(400, response.getStatusLine().getStatusCode());
        Assert.assertTrue(responseString.contains("Invalid Data:"));
    }

    @Test
    public void testIngestingInvalidAnnotationsData() throws Exception {
        URIBuilder builder = new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(httpPort).setPath("/v2.0/456854/events");
        HttpPost post = new HttpPost(builder.build());
        String requestBody = //Invalid Data.
                "{\"how\":346550008," +
                        "\"why\":\"Dummy Event\"," +
                        "\"info\":\"Dummy Data\"," +
                        "\"tickets\":\"deployment\"}";
        HttpEntity entity = new StringEntity(requestBody,
                ContentType.APPLICATION_JSON);
        post.setEntity(entity);
        post.setHeader(Event.FieldLabels.tenantId.name(), "456854");
        HttpResponse response = client.execute(post);
        String responseString = EntityUtils.toString(response.getEntity());
        Assert.assertEquals(400, response.getStatusLine().getStatusCode());
        Assert.assertTrue(responseString.contains("Invalid Data:"));
    }

    @Test
    public void testHttpAggregatedIngestionHappyCase() throws Exception {
        StringBuilder sb = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("src/test/resources/sample_payload.json")));
        String curLine = reader.readLine();
        while (curLine != null) {
            sb = sb.append(curLine);
            curLine = reader.readLine();
        }
        String json = sb.toString();

        URIBuilder builder = getMetricsURIBuilder()
                .setPath("/v2.0/333333/ingest/aggregated");
        HttpPost post = new HttpPost(builder.build());
        HttpEntity entity = new StringEntity(json, ContentType.APPLICATION_JSON);
        post.setEntity(entity);
        HttpResponse response = client.execute(post);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        verify(context, atLeastOnce()).update(anyLong(), anyInt());
        final Locator locator = Locator.
                createLocatorFromPathComponents("333333", "internal", "packets_received");
        Points<BluefloodCounterRollup> points = AstyanaxReader.getInstance().getDataToRoll(BluefloodCounterRollup.class,
                locator, new Range(1389211220,1389211240),
                CassandraModel.getColumnFamily(BluefloodCounterRollup.class, Granularity.FULL));
        Assert.assertEquals(1, points.getPoints().size());
        EntityUtils.consume(response.getEntity()); // Releases connection apparently
    }

    @Test
    public void testHttpAggregatedMultiIngestionHappyCase() throws Exception {
        StringBuilder sb = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("src/test/resources/sample_multi_bundle.json")));
        String curLine = reader.readLine();
        while (curLine != null) {
            sb = sb.append(curLine);
            curLine = reader.readLine();
        }
        String json = sb.toString();

        URIBuilder builder = getMetricsURIBuilder()
                .setPath("/v2.0/333333/ingest/aggregated/multi");
        HttpPost post = new HttpPost(builder.build());
        HttpEntity entity = new StringEntity(json, ContentType.APPLICATION_JSON);
        post.setEntity(entity);
        HttpResponse response = client.execute(post);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        verify(context, atLeastOnce()).update(anyLong(), anyInt());

        final Locator locator = Locator.createLocatorFromPathComponents("5405532", "G200ms");
        Points<BluefloodGaugeRollup> points = AstyanaxReader.getInstance().getDataToRoll(BluefloodGaugeRollup.class,
                locator, new Range(1439231323000L, 1439231325000L), CassandraModel.getColumnFamily(BluefloodGaugeRollup.class, Granularity.FULL));
        Assert.assertEquals(1, points.getPoints().size());

        final Locator locator1 = Locator.createLocatorFromPathComponents("5405577", "internal.bad_lines_seen");
        Points<BluefloodCounterRollup> points1 = AstyanaxReader.getInstance().getDataToRoll(BluefloodCounterRollup.class,
                locator1, new Range(1439231323000L, 1439231325000L), CassandraModel.getColumnFamily(BluefloodCounterRollup.class, Granularity.FULL));
        Assert.assertEquals(1, points1.getPoints().size());

        EntityUtils.consume(response.getEntity()); // Releases connection apparently
    }

    @Test
    public void testBadRequests() throws Exception {
        HttpPost post = new HttpPost(getMetricsURI());
        HttpResponse response = client.execute(post);  // no body
        Assert.assertEquals(response.getStatusLine().getStatusCode(), 400);
        EntityUtils.consume(response.getEntity()); // Releases connection apparently

        post = new HttpPost(getMetricsURI());
        HttpEntity entity = new StringEntity("Some incompatible json body", ContentType.APPLICATION_JSON);
        post.setEntity(entity);
        response = client.execute(post);
        Assert.assertEquals(response.getStatusLine().getStatusCode(), 400);
        EntityUtils.consume(response.getEntity()); // Releases connection apparently
    }

    @Test
    public void testCompressedRequests() throws Exception{
        HttpPost post = new HttpPost(getMetricsURI());
        String content = JSONMetricsContainerTest.generateJSONMetricsData();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(content.length());
        GZIPOutputStream gzipOut = new GZIPOutputStream(baos);
        gzipOut.write(content.getBytes());
        gzipOut.close();
        ByteArrayEntity entity = new ByteArrayEntity(baos.toByteArray());
        //Setting the content encoding to gzip
        entity.setContentEncoding("gzip");
        baos.close();
        post.setEntity(entity);
        HttpResponse response = client.execute(post);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        EntityUtils.consume(response.getEntity()); // Releases connection apparently
    }

    @Test
    public void testMultiTenantBatching() throws Exception{
        URIBuilder builder = getMetricsURIBuilder()
                .setPath("/v2.0/acTest/ingest/multi");
        HttpPost post = new HttpPost(builder.build());
        String content = JSONMetricsContainerTest.generateMultitenantJSONMetricsData();
        HttpEntity entity = new StringEntity(content,
                ContentType.APPLICATION_JSON);
        post.setEntity(entity);
        HttpResponse response = client.execute(post);

        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        verify(context, atLeastOnce()).update(anyLong(), anyInt());
        // assert that the update method on the ScheduleContext object was called and completed successfully
        // Now read the metrics back from dcass and check (relies on generareJSONMetricsData from JSONMetricsContainerTest)
        final Locator locator = Locator.createLocatorFromPathComponents("tenantOne", "mzord.duration");
        Points<SimpleNumber> points = AstyanaxReader.getInstance().getDataToRoll(SimpleNumber.class,
                locator, new Range(1234567878, 1234567900), CassandraModel.getColumnFamily(BasicRollup.class, Granularity.FULL));
        Assert.assertEquals(1, points.getPoints().size());

        final Locator locatorTwo = Locator.createLocatorFromPathComponents("tenantTwo", "mzord.duration");
        Points<SimpleNumber> pointsTwo = AstyanaxReader.getInstance().getDataToRoll(SimpleNumber.class,
                locator, new Range(1234567878, 1234567900), CassandraModel.getColumnFamily(BasicRollup.class, Granularity.FULL));
        Assert.assertEquals(1, pointsTwo.getPoints().size());

        EntityUtils.consume(response.getEntity()); // Releases connection apparently
    }

    @Test
    public void testMultiTenantFailureForSingleTenantHandler() throws Exception {
        URIBuilder builder = getMetricsURIBuilder()
                .setPath("/v2.0/acTest/ingest");
        HttpPost post = new HttpPost(builder.build());
        String content = JSONMetricsContainerTest.generateMultitenantJSONMetricsData();
        HttpEntity entity = new StringEntity(content,
                ContentType.APPLICATION_JSON);
        post.setEntity(entity);
        HttpResponse response = client.execute(post);

        Assert.assertEquals(400, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testMultiTenantFailureWithoutTenant() throws Exception {
        // 400 if sending for other tenants without actually stamping a tenant id on the incoming metrics
        URIBuilder builder = getMetricsURIBuilder()
                .setPath("/v2.0/acTest/ingest/multi");
        HttpPost post = new HttpPost(builder.build());
        String content = JSONMetricsContainerTest.generateJSONMetricsData();
        HttpEntity entity = new StringEntity(content,
                ContentType.APPLICATION_JSON);
        post.setEntity(entity);
        HttpResponse response = client.execute(post);

        Assert.assertEquals(400, response.getStatusLine().getStatusCode());
    }

    private URI getMetricsURI() throws URISyntaxException {
        return getMetricsURIBuilder().build();
    }

    private URIBuilder getMetricsURIBuilder() throws URISyntaxException {
        return new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(httpPort).setPath("/v2.0/acTEST/ingest");
    }

    private static void createAndInsertTestEvents(final String tenant, int eventCount) throws Exception {
        ArrayList<Map<String, Object>> eventList = new ArrayList<Map<String, Object>>();
        for (int i=0; i<eventCount; i++) {
            Event event = new Event();
            event.setWhat("deployment");
            event.setWhen(Calendar.getInstance().getTimeInMillis());
            event.setData("deploying prod");
            event.setTags("deployment");

            eventList.add(event.toMap());
        }
        eventsSearchIO.insert(tenant, eventList);
    }

    private HttpResponse postEvent(String requestBody, String tenantId) throws Exception {
        URIBuilder builder = new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(httpPort).setPath("/v2.0/" + tenantId + "/events");
        HttpPost post = new HttpPost(builder.build());
        HttpEntity entity = new StringEntity(requestBody,
                ContentType.APPLICATION_JSON);
        post.setEntity(entity);
        post.setHeader(Event.FieldLabels.tenantId.name(), tenantId);
        HttpResponse response = client.execute(post);
        return response;
    }

    private static String createTestEvent(int batchSize) throws Exception {
        StringBuilder events = new StringBuilder();
        for (int i=0; i<batchSize; i++) {
            Event event = new Event();
            event.setWhat("deployment "+i);
            event.setWhen(Calendar.getInstance().getTimeInMillis());
            event.setData("deploying prod "+i);
            event.setTags("deployment "+i);
            events.append(new ObjectMapper().writeValueAsString(event));
        }
        return events.toString();
    }
    
    @AfterClass
    public static void shutdown() {
        esSetup.terminate();
        vendor.shutdown();
    }
}