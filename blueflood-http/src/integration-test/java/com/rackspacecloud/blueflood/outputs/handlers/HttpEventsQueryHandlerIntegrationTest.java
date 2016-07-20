/*
 * Copyright 2013-2016 Rackspace
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

import com.rackspacecloud.blueflood.http.HttpIntegrationTestBase;
import com.rackspacecloud.blueflood.types.Event;
import org.apache.http.util.EntityUtils;
import org.junit.*;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;

import java.util.*;

/**
 * Integration Tests for GET .../events/getEvents
 */
public class HttpEventsQueryHandlerIntegrationTest extends HttpIntegrationTestBase {

    private final String tenantId = "540123";

    @Before
    public void setup() throws Exception {
        createAndInsertTestEvents(tenantId, 5);
        esSetup.client().admin().indices().prepareRefresh().execute().actionGet();
    }

    @Test
    public void testHttpEventsQueryHandler_HappyCase() throws Exception {
        parameterMap = new HashMap<String, String>();
        parameterMap.put(Event.fromParameterName, String.valueOf(baseMillis - 86400000));
        parameterMap.put(Event.untilParameterName, String.valueOf(baseMillis + (86400000*3)));
        HttpGet get = new HttpGet(getQueryEventsURI(tenantId));
        HttpResponse response = client.execute(get);

        String responseString = EntityUtils.toString(response.getEntity());
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        Assert.assertFalse(responseString.equals("[]"));
        assertResponseHeaderAllowOrigin(response);
    }

    @Test
    public void testHttpEventsQueryHandler_StaleTimeStamps() throws Exception {
        parameterMap = new HashMap<String, String>();
        parameterMap.put(Event.fromParameterName, String.valueOf(baseMillis - 86400000));
        parameterMap.put(Event.untilParameterName, String.valueOf(baseMillis));
        HttpGet get = new HttpGet(getQueryEventsURI(tenantId));
        HttpResponse response = client.execute(get);
        String responseString = EntityUtils.toString(response.getEntity());
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        Assert.assertNotNull(responseString);
        Assert.assertTrue(responseString.equals("[]"));
        assertResponseHeaderAllowOrigin(response);
    }

    @Test
    public void testHttpEventsQueryHandler_ByTagName() throws Exception {
        parameterMap = new HashMap<String, String>();
        parameterMap.put(Event.tagsParameterName, "1");
        HttpGet get = new HttpGet(getQueryEventsURI(tenantId));
        HttpResponse response = client.execute(get);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        String responseString = EntityUtils.toString(response.getEntity());
        Assert.assertNotNull(responseString);
        Assert.assertFalse(responseString.equals("[]"));

        //Test Using non-existing tag name
        parameterMap.put(Event.tagsParameterName, "NoSuchTag");
        get = new HttpGet(getQueryEventsURI(tenantId));
        response = client.execute(get);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        responseString = EntityUtils.toString(response.getEntity());
        Assert.assertNotNull(responseString);
        Assert.assertTrue(responseString.equals("[]"));
    }

    @Test
    public void testHttpEventsQueryHandler_MultipleTagsReturnNothing() throws Exception {
        parameterMap = new HashMap<String, String>();
        parameterMap.put(Event.tagsParameterName, "0,1");
        HttpGet get = new HttpGet(getQueryEventsURI(tenantId));
        HttpResponse response = client.execute(get);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        String responseString = EntityUtils.toString(response.getEntity());
        Assert.assertNotNull(responseString);
        Assert.assertTrue(responseString.equals("[]"));

        parameterMap.put(Event.tagsParameterName, "[0,1]");
        get = new HttpGet(getQueryEventsURI(tenantId));
        response = client.execute(get);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        responseString = EntityUtils.toString(response.getEntity());
        Assert.assertNotNull(responseString);
        Assert.assertTrue(responseString.equals("[]"));

        parameterMap.put(Event.tagsParameterName, "{0,1}");
        get = new HttpGet(getQueryEventsURI(tenantId));
        response = client.execute(get);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        responseString = EntityUtils.toString(response.getEntity());
        Assert.assertNotNull(responseString);
        Assert.assertTrue(responseString.equals("[]"));
    }

    @Test
    public void testHttpEventsQueryHandler_WildcardTagReturnNothing() throws Exception {
        parameterMap = new HashMap<String, String>();
        parameterMap.put(Event.tagsParameterName, "sample*");

        HttpGet get = new HttpGet(getQueryEventsURI(tenantId));
        HttpResponse response = client.execute(get);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        String responseString = EntityUtils.toString(response.getEntity());
        Assert.assertNotNull(responseString);
        Assert.assertTrue(responseString.equals("[]"));
        assertResponseHeaderAllowOrigin(response);
    }

    @Test
    public void testHttpEventsQueryHandler_NoParams() throws Exception {
        parameterMap = new HashMap<String, String>();
        HttpGet get = new HttpGet(getQueryEventsURI(tenantId));
        HttpResponse response = client.execute(get);
        String responseString = EntityUtils.toString(response.getEntity());
        Assert.assertEquals(400, response.getStatusLine().getStatusCode());
        Assert.assertNotNull(responseString);
        Assert.assertTrue(responseString.contains("Query should contain at least one query parameter"));
        assertResponseHeaderAllowOrigin(response);
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
        eventsSearchIO.insert(tenant, eventList);
    }

}
