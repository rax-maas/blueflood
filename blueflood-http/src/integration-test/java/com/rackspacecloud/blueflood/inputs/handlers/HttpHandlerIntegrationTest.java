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

import com.rackspacecloud.blueflood.http.HttpClientVendor;
import com.rackspacecloud.blueflood.inputs.formats.JSONMetricsContainerTest;
import com.rackspacecloud.blueflood.io.AstyanaxMetricsWriter;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.HttpConfig;
import com.rackspacecloud.blueflood.service.HttpIngestionService;
import com.rackspacecloud.blueflood.service.ScheduleContext;
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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashSet;
import java.util.zip.GZIPOutputStream;

import static org.mockito.Mockito.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class HttpHandlerIntegrationTest {
    private static HttpIngestionService httpIngestionService;
    private static HttpClientVendor vendor;
    private static DefaultHttpClient client;
    private static Collection<Integer> manageShards = new HashSet<Integer>();
    private static int httpPort;
    private static ScheduleContext context;

    @BeforeClass
    public static void setUp() {
        httpPort = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_INGESTION_PORT);
        manageShards.add(1); manageShards.add(5); manageShards.add(6);
        context = spy(new ScheduleContext(System.currentTimeMillis(), manageShards));
        httpIngestionService = new HttpIngestionService();
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
    public void testHttpAggregatedIngestionHappyCase() throws Exception {

        StringBuilder sb = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("src/test/resources/sample_bundle.json")));
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
        Points<CounterRollup> points = AstyanaxReader.getInstance().getDataToRoll(CounterRollup.class,
                locator, new Range(1389211220,1389211240), 
                CassandraModel.getColumnFamily(CounterRollup.class, Granularity.FULL));
        Assert.assertEquals(1, points.getPoints().size());
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

    @AfterClass
    public static void shutdown() {
        vendor.shutdown();
    }
}
