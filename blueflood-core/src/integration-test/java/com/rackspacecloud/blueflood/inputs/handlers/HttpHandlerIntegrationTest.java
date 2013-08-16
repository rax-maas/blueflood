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

package com.rackspacecloud.blueflood.inputs.handlers;

import com.netflix.astyanax.model.ColumnList;
import com.rackspacecloud.blueflood.http.HttpClientVendor;
import com.rackspacecloud.blueflood.inputs.formats.JSONMetricsContainerTest;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.ScheduleContext;
import com.rackspacecloud.blueflood.types.Locator;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashSet;

public class HttpHandlerIntegrationTest {
    private static HttpMetricsIngestionServer httpMetricsIngestorServer;
    private static HttpClientVendor vendor;
    private static DefaultHttpClient client;
    private static Collection<Integer> manageShards = new HashSet<Integer>();
    private static int httpPort;

    @BeforeClass
    public static void setUp() {
        httpPort = Configuration.getIntegerProperty("HTTP_INGESTION_PORT");
        manageShards.add(1); manageShards.add(5); manageShards.add(6);
        ScheduleContext context = new ScheduleContext(System.currentTimeMillis(), manageShards);
        httpMetricsIngestorServer = new HttpMetricsIngestionServer(httpPort, context);
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
        Assert.assertEquals(response.getStatusLine().getStatusCode(), 200);
        // Now read the metrics back from dcass and check (relies on generareJSONMetricsData from JSONMetricsContainerTest)
        final Locator locator = Locator.createLocatorFromPathComponents("acTEST", "mzord.duration");
        ColumnList<Long> rollups = AstyanaxReader.getInstance().getNumericRollups(locator, Granularity.FULL, 1234567878, 1234567900);
        Assert.assertEquals(1, rollups.size());
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

    private URI getMetricsURI() throws URISyntaxException {
        URIBuilder builder = new URIBuilder().setScheme("http").setHost("127.0.0.1")
                                             .setPort(httpPort).setPath("/v1.0/acTEST/experimental/metrics");
        return builder.build();
    }

    @AfterClass
    public static void shutdown() {
        vendor.shutdown();
    }
}
