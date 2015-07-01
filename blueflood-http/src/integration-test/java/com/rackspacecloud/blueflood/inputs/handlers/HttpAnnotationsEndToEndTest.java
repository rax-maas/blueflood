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
import com.rackspacecloud.blueflood.io.AstyanaxMetricsWriter;
import com.rackspacecloud.blueflood.io.EventElasticSearchIO;
import com.rackspacecloud.blueflood.io.EventsIO;
import com.rackspacecloud.blueflood.outputs.handlers.HttpMetricDataQueryServer;
import com.rackspacecloud.blueflood.service.*;
import com.rackspacecloud.blueflood.types.Event;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static org.mockito.Mockito.spy;

public class HttpAnnotationsEndToEndTest {
    private static HttpIngestionService httpIngestionService;
    private static HttpClientVendor vendor;
    private static DefaultHttpClient client;
    private static Collection<Integer> manageShards = new HashSet<Integer>();
    private static int httpPort;
    private static int queryPort;
    private static ScheduleContext context;
    private static EventsIO eventsSearchIO;
    private static HttpQueryService httpQueryService;
    private static  Map <String, String> parameterMap;
    private final String tenant_id = "333333";
    private static EsSetup esSetup;
    //A time stamp 2 days ago
    private final long baseMillis = Calendar.getInstance().getTimeInMillis() - 172800000;

    @BeforeClass
    public static void setUp() throws Exception {
        System.setProperty(CoreConfig.EVENTS_MODULES.name(), "com.rackspacecloud.blueflood.io.EventElasticSearchIO");
        Configuration.getInstance().init();
        httpPort = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_INGESTION_PORT);
        queryPort = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_METRIC_DATA_QUERY_PORT);
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

        httpQueryService = new HttpQueryService();
        HttpMetricDataQueryServer queryServer = new HttpMetricDataQueryServer();
        queryServer.setEventsIO(eventsSearchIO);
        httpQueryService.setServer(queryServer);
        httpQueryService.startService();

        vendor = new HttpClientVendor();
        client = vendor.getClient();
    }

    @Test
    public void testAnnotationsEndToEndHappyCase() throws Exception {
        final int batchSize = 1;
        String event = createTestEvent(batchSize);
        HttpResponse response = postEvent(event, tenant_id);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        Thread.sleep(1000);

        parameterMap = new HashMap<String, String>();
        parameterMap.put(Event.fromParameterName, String.valueOf(baseMillis - 86400000));
        parameterMap.put(Event.untilParameterName, String.valueOf(baseMillis + (86400000*3)));
        HttpGet get = new HttpGet(getAnnotationsQueryURI());
        response = client.execute(get);

        String responseString = EntityUtils.toString(response.getEntity());
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        Assert.assertFalse(responseString.equals("[]"));
        Assert.assertTrue(responseString.contains("deployment 0"));
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

    private URI getAnnotationsQueryURI() throws URISyntaxException {
        URIBuilder builder = new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(queryPort).setPath("/v2.0/" + tenant_id + "/events/getEvents");

        Set<String> parameters = parameterMap.keySet();
        Iterator<String> setIterator = parameters.iterator();
        while (setIterator.hasNext()){
            String paramName = setIterator.next();
            builder.setParameter(paramName, parameterMap.get(paramName));
        }
        return builder.build();
    }

    @AfterClass
    public static void tearDownClass() throws Exception{
        Configuration.getInstance().setProperty(CoreConfig.EVENTS_MODULES.name(), "");
        esSetup.terminate();
        httpQueryService.stopService();
    }
}
