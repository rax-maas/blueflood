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
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Calendar;
import java.util.HashMap;

/**
 * Integration Tests for GET .../events/getEvents
 */
public class HttpEventsQueryHandlerIntegrationTest extends HttpIntegrationTestBase {

    private static final String tenantId = "540123";

    @BeforeClass
    public static void setup() throws Exception {
        createAndInsertTestEvents(tenantId, 5);
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
        parameterMap = new HashMap<>();
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
        parameterMap = new HashMap<>();
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
        parameterMap = new HashMap<>();
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
        parameterMap = new HashMap<>();
        HttpGet get = new HttpGet(getQueryEventsURI(tenantId));
        HttpResponse response = client.execute(get);
        String responseString = EntityUtils.toString(response.getEntity());
        Assert.assertEquals(400, response.getStatusLine().getStatusCode());
        Assert.assertNotNull(responseString);
        Assert.assertTrue(responseString.contains("Query should contain at least one query parameter"));
        assertResponseHeaderAllowOrigin(response);
    }

    private static void createAndInsertTestEvents(final String tenant, int eventCount) throws Exception {
        for (int i=0; i<eventCount; i++) {
            Event event = new Event();
            event.setWhat(String.format("[%s] %s %d", tenant, "Event title sample", i));
            event.setWhen(Calendar.getInstance().getTimeInMillis());
            event.setData(String.format("[%s] %s %d", tenant, "Event data sample", i));
            event.setTags(String.format("[%s] %s %d", tenant, "Event tags sample", i));
            eventsSearchIO.insert(tenant, event.toMap());
        }
    }

    /*
    Once done testing, delete all of the records of the given type and index.
    NOTE: Don't delete the index or the type, because that messes up the ES settings.
     */
    @AfterClass
    public static void tearDownClass() throws Exception {
        URIBuilder builder = new URIBuilder().setScheme("http")
                .setHost("127.0.0.1").setPort(9200)
                .setPath("/events/graphite_event/_query");

        HttpEntityEnclosingRequestBase delete = new HttpEntityEnclosingRequestBase() {
            @Override
            public String getMethod() {
                return "DELETE";
            }
        };
        delete.setURI(builder.build());

        String deletePayload = "{\"query\":{\"match_all\":{}}}";
        HttpEntity entity = new NStringEntity(deletePayload, ContentType.APPLICATION_JSON);
        delete.setEntity(entity);

        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = client.execute(delete);
        if(response.getStatusLine().getStatusCode() != 200)
        {
            System.out.println("Couldn't delete index after running tests.");
        }
        else {
            System.out.println("Successfully deleted index after running tests.");
        }
    }
}
