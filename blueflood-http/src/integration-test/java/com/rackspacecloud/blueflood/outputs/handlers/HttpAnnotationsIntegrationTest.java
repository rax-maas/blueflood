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

package com.rackspacecloud.blueflood.outputs.handlers;

import com.github.tlrx.elasticsearch.test.EsSetup;
import com.rackspacecloud.blueflood.http.HttpClientVendor;
import com.rackspacecloud.blueflood.io.EventElasticSearchIO;
import com.rackspacecloud.blueflood.service.*;
import com.rackspacecloud.blueflood.types.Event;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.*;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class HttpAnnotationsIntegrationTest {
    //A time stamp 2 days ago
    private final long baseMillis = Calendar.getInstance().getTimeInMillis() - 172800000;
    private final String tenantId = "540123";
    private static int queryPort;
    private static EventElasticSearchIO eventsIO;
    private static EsSetup esSetup;
    private static HttpQueryService httpQueryService;
    private static HttpClientVendor vendor;
    private static DefaultHttpClient client;
    private static  Map <String, String> parameterMap;

    @BeforeClass
    public static void setUpHttp() throws Exception {
        queryPort = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_METRIC_DATA_QUERY_PORT);
        vendor = new HttpClientVendor();
        client = vendor.getClient();
        esSetup = new EsSetup();
        esSetup.execute(EsSetup.deleteAll());
        esSetup.execute(EsSetup.createIndex(EventElasticSearchIO.EVENT_INDEX)
                .withSettings(EsSetup.fromClassPath("index_settings.json"))
                .withMapping("annotations", EsSetup.fromClassPath("events_mapping.json")));
        eventsIO = new EventElasticSearchIO(esSetup.client());

        httpQueryService = new HttpQueryService();
        HttpMetricDataQueryServer queryServer = new HttpMetricDataQueryServer();
        queryServer.setEventsIO(eventsIO);
        httpQueryService.setServer(queryServer);
        httpQueryService.startService();
    }

    @Before
    public void setup() throws Exception {
        createAndInsertTestEvents(tenantId, 5);
        esSetup.client().admin().indices().prepareRefresh().execute().actionGet();
    }

    @Test
    public void testHttpQueryAnnotationsHappyCase() throws Exception {
        parameterMap = new HashMap<String, String>();
        parameterMap.put(Event.fromParameterName, String.valueOf(baseMillis - 86400000));
        parameterMap.put(Event.untilParameterName, String.valueOf(baseMillis + (86400000*3)));
        HttpGet get = new HttpGet(getAnnotationsQueryURI());
        HttpResponse response = client.execute(get);

        String responseString = EntityUtils.toString(response.getEntity());
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        Assert.assertFalse(responseString.equals("[]"));
    }

    @Test
    public void testQueryHttpAnnotationsStaleTimeStamps() throws Exception {
        parameterMap = new HashMap<String, String>();
        parameterMap.put(Event.fromParameterName, String.valueOf(baseMillis - 86400000));
        parameterMap.put(Event.untilParameterName, String.valueOf(baseMillis));
        HttpGet get = new HttpGet(getAnnotationsQueryURI());
        HttpResponse response = client.execute(get);
        String responseString = EntityUtils.toString(response.getEntity());
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        Assert.assertNotNull(responseString);
        Assert.assertTrue(responseString.equals("[]"));
    }

    @Test
    public void testQueryAnnotationsByTagName() throws Exception {
        parameterMap = new HashMap<String, String>();
        parameterMap.put(Event.tagsParameterName, "1");
        HttpGet get = new HttpGet(getAnnotationsQueryURI());
        HttpResponse response = client.execute(get);
        String responseString = EntityUtils.toString(response.getEntity());
        Assert.assertNotNull(responseString);
        Assert.assertFalse(responseString.equals("[]"));

        //Test Using non-existing tag name
        parameterMap.put(Event.tagsParameterName, "NoSuchTag");
        get = new HttpGet(getAnnotationsQueryURI());
        response = client.execute(get);
        responseString = EntityUtils.toString(response.getEntity());
        Assert.assertNotNull(responseString);
        Assert.assertTrue(responseString.equals("[]"));
    }

    @Test
    public void testQueriesByMultipleTagsReturnNothing() throws Exception {
        parameterMap = new HashMap<String, String>();
        parameterMap.put(Event.tagsParameterName, "0,1");
        HttpGet get = new HttpGet(getAnnotationsQueryURI());
        HttpResponse response = client.execute(get);
        String responseString = EntityUtils.toString(response.getEntity());
        Assert.assertNotNull(responseString);
        Assert.assertTrue(responseString.equals("[]"));

        parameterMap.put(Event.tagsParameterName, "[0,1]");
        get = new HttpGet(getAnnotationsQueryURI());
        response = client.execute(get);
        responseString = EntityUtils.toString(response.getEntity());
        Assert.assertNotNull(responseString);
        Assert.assertTrue(responseString.equals("[]"));

        parameterMap.put(Event.tagsParameterName, "{0,1}");
        get = new HttpGet(getAnnotationsQueryURI());
        response = client.execute(get);
        responseString = EntityUtils.toString(response.getEntity());
        Assert.assertNotNull(responseString);
        Assert.assertTrue(responseString.equals("[]"));
    }

    @Test
    public void testWildcardTagQueriesReturnNothing() throws Exception {
        parameterMap = new HashMap<String, String>();
        parameterMap.put(Event.tagsParameterName, "sample*");

        HttpGet get = new HttpGet(getAnnotationsQueryURI());
        HttpResponse response = client.execute(get);
        String responseString = EntityUtils.toString(response.getEntity());
        Assert.assertNotNull(responseString);
        Assert.assertTrue(responseString.equals("[]"));
    }

    @Test
    public void testQueryHttpAnnotationsNoParams() throws Exception {
        parameterMap = new HashMap<String, String>();
        HttpGet get = new HttpGet(getAnnotationsQueryURI());
        HttpResponse response = client.execute(get);
        String responseString = EntityUtils.toString(response.getEntity());
        Assert.assertEquals(400, response.getStatusLine().getStatusCode());
        Assert.assertNotNull(responseString);
        Assert.assertTrue(responseString.contains("Query should contain at least one query parameter"));
    }

    private URI getAnnotationsQueryURI() throws URISyntaxException {
        URIBuilder builder = new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(queryPort).setPath("/v2.0/" + tenantId + "/events/getEvents");

        Set<String> parameters = parameterMap.keySet();
        Iterator<String> setIterator = parameters.iterator();
        while (setIterator.hasNext()){
            String paramName = setIterator.next();
            builder.setParameter(paramName, parameterMap.get(paramName));
        }
        return builder.build();
    }

    private static void createAndInsertTestEvents(final String tenant, int eventCount) throws Exception {
        ArrayList<Map<String, Object>> eventList = new ArrayList<Map<String, Object>>();
        for (int i=0; i<eventCount; i++) {
            Event event = new Event();
            event.setWhat(String.format("[%s] %s %d", tenant, "Event title sample", i));
            event.setWhen(Calendar.getInstance().getTimeInMillis());
            event.setData(String.format("[%s] %s %d", tenant, "Event data sample", i));
            event.setTags(String.format("[%s] %s %d", tenant, "Event tags sample", i));
            eventList.add(event.toMap());
        }
        eventsIO.insert(tenant, eventList);
    }

    @AfterClass
    public static void tearDownClass() throws Exception{
        Configuration.getInstance().setProperty(CoreConfig.EVENTS_MODULES.name(), "");
        esSetup.terminate();
        httpQueryService.stopService();
    }
}
