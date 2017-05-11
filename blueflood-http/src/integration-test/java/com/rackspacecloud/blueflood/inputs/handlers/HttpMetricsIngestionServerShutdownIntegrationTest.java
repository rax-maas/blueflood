/*
 * Copyright 2015 Rackspace
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


import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.rackspacecloud.blueflood.http.HttpClientVendor;
import com.rackspacecloud.blueflood.http.HttpESIntegrationBase;
import com.rackspacecloud.blueflood.io.EventElasticSearchIO;
import com.rackspacecloud.blueflood.io.EventsIO;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.service.HttpConfig;
import com.rackspacecloud.blueflood.service.ScheduleContext;
import com.rackspacecloud.blueflood.utils.ModuleLoader;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.*;

import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;

import static com.rackspacecloud.blueflood.TestUtils.generateJSONMetricsData;
import static org.mockito.Mockito.spy;

/**
 *
 * The current scope gives us one cluster for all test methods in the test.
 * All indices and templates are deleted between each test.
 *
 * The following flags have to be set while running this test
 * -Dtests.jarhell.check=false (to handle some bug in intellij https://github.com/elastic/elasticsearch/issues/14348)
 * -Dtests.security.manager=false (https://github.com/elastic/elasticsearch/issues/16459)
 *
 */
public class HttpMetricsIngestionServerShutdownIntegrationTest extends HttpESIntegrationBase {

    private static HttpMetricsIngestionServer server;
    private static HttpClientVendor vendor;
    private static DefaultHttpClient client;
    private static Collection<Integer> manageShards = new HashSet<Integer>();
    private static int httpPort;
    private static ScheduleContext context;
    private static EventsIO eventsSearchIO;
    //A time stamp 2 days ago
    private final long baseMillis = Calendar.getInstance().getTimeInMillis() - 172800000;


    @BeforeClass
    public static void setUpHttpService() throws Exception{
        System.setProperty(CoreConfig.EVENTS_MODULES.name(), "com.rackspacecloud.blueflood.io.EventElasticSearchIO");
        Configuration.getInstance().init();
        httpPort = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_INGESTION_PORT);
        manageShards.add(1); manageShards.add(5); manageShards.add(6);
        context = spy(new ScheduleContext(System.currentTimeMillis(), manageShards));

        vendor = new HttpClientVendor();
        client = vendor.getClient();
    }

    @Before
    public void setup() throws Exception {
        // setup elasticsearch test clusters with blueflood mappings
        createIndexAndMapping(EventElasticSearchIO.EVENT_INDEX,
                              "graphite_event",
                              getEventsMapping());

        eventsSearchIO = new EventElasticSearchIO(getClient());
        ((EventElasticSearchIO) ModuleLoader.getInstance(EventElasticSearchIO.class, CoreConfig.EVENTS_MODULES)).setClient(getClient());

        server = new HttpMetricsIngestionServer(context);
        server.setHttpEventsIngestionHandler(new HttpEventsIngestionHandler(eventsSearchIO));
        server.startServer();

        refreshChanges();
    }

    @Test
    public void testHttpIngestionHappyCase() throws Exception {

        // given
        HttpPost post = new HttpPost(getMetricsURI());
        HttpEntity entity = new StringEntity( generateJSONMetricsData(),
                ContentType.APPLICATION_JSON);
        post.setEntity(entity);
        HttpResponse response = client.execute(post);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        HttpPost post2 = new HttpPost(getMetricsURI());
        HttpEntity entity2 = new StringEntity( generateJSONMetricsData(),
                ContentType.APPLICATION_JSON);
        post2.setEntity(entity2);

        // when
        server.shutdownServer();

        // then
        try {
            HttpResponse response2 = client.execute(post2);
            Assert.fail("We should have received a Connect exception");
        } catch (ConnectException ex) {

            // NOTE: ideally, one would simply use jUnit's `ExpectedException`
            // rule or `expected` param to indicate that we expected a
            // ConnectException to be thrown. However, ConnectException can be
            // thrown for a number of different reasons, and the only way to
            // know for sure that the connection was refused (and, thus, that
            // the port is no longer open) is to catch the exception object and
            // check its message. Hence, this try/catch.

            Assert.assertTrue(ex.getMessage().contains("Connection refused"));
        }
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
        Configuration.getInstance().setProperty(CoreConfig.EVENTS_MODULES.name(), "");
        System.clearProperty(CoreConfig.EVENTS_MODULES.name());

        if (vendor != null) {
            vendor.shutdown();
        }
    }

}