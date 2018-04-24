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

package com.rackspacecloud.blueflood.io;

import com.github.tlrx.elasticsearch.test.EsSetup;
import com.rackspacecloud.blueflood.types.Event;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.nio.entity.NStringEntity;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

public class EventElasticSearchIOTest {
    private static EventElasticSearchIO searchIO;
    private static EsSetup esSetup;

    private static final String TENANT_1 = "tenant1";
    private static final String TENANT_2 = "otheruser2";
    private static final String TENANT_RANGE = "rangetenant";
    private static final String TENANT_WITH_SYMBOLS = "tenant-id_id#id";
    private static final int TENANT_1_EVENTS_NUM = 3;
    private static final int TENANT_2_EVENTS_NUM = 7;
    private static final int TENANT_WITH_SYMBOLS_NUM = 2;
    private static final int TENANT_RANGE_EVENTS_NUM = 10;
    private static final int RANGE_STEP_IN_SECONDS = 15 * 60;


    @Test
    public void testNonCrossTenantSearch() throws Exception {
        Map<String, List<String>> query = new HashMap<>();
        query.put(Event.tagsParameterName, Arrays.asList("event"));
        List<Map<String, Object>> results = searchIO.search(TENANT_1, query);
        Assert.assertEquals(TENANT_1_EVENTS_NUM, results.size());

        results = searchIO.search(TENANT_2, query);
        Assert.assertEquals(TENANT_2_EVENTS_NUM, results.size());

        results = searchIO.search(TENANT_RANGE, query);
        Assert.assertEquals(TENANT_RANGE_EVENTS_NUM, results.size());

        results = searchIO.search(TENANT_WITH_SYMBOLS, query);
        Assert.assertEquals(TENANT_WITH_SYMBOLS_NUM, results.size());
    }

    @Test
    public void testEmptyQueryParameters() throws Exception {
        Map<String, List<String>> query = new HashMap<>();
        query.put(Event.tagsParameterName, new ArrayList<>());
        query.put(Event.fromParameterName, new ArrayList<>());
        query.put(Event.untilParameterName, new ArrayList<>());

        List<Map<String, Object>> results = searchIO.search(TENANT_1, query);
        Assert.assertEquals(TENANT_1_EVENTS_NUM, results.size());
    }

    @Test
    public void testEventTagsOnlySearch() throws Exception {
        Map<String, List<String>> query = new HashMap<>();
        query.put(Event.tagsParameterName, Arrays.asList("sample"));
        List<Map<String, Object>> results = searchIO.search(TENANT_1, query);
        Assert.assertEquals(TENANT_1_EVENTS_NUM, results.size());

        query.put(Event.tagsParameterName, Arrays.asList("1"));
        results = searchIO.search(TENANT_1, query);
        Assert.assertEquals(1, results.size());

        query.put(Event.tagsParameterName, Arrays.asList("database"));
        results = searchIO.search(TENANT_1, query);
        Assert.assertEquals(0, results.size());
    }

    @Test
    public void testEmptyQuery() throws Exception {
        List<Map<String, Object>> results = searchIO.search(TENANT_1, null);
        Assert.assertEquals(TENANT_1_EVENTS_NUM, results.size());
    }

    @Test
    public void testRangeOnlySearch() throws Exception {
        Map<String, List<String>> query = new HashMap<>();
        final int eventCountToCapture = TENANT_RANGE_EVENTS_NUM / 2;
        final int secondsDelta = 10;
        DateTime fromDateTime = new DateTime().minusSeconds(RANGE_STEP_IN_SECONDS * eventCountToCapture - secondsDelta);
        query.put(Event.fromParameterName, Arrays.asList(Long.toString(fromDateTime.getMillis())));
        List<Map<String, Object>> results = searchIO.search(TENANT_RANGE, query);
        Assert.assertEquals(eventCountToCapture, results.size());

        DateTime untilDateTime = new DateTime().minusSeconds(RANGE_STEP_IN_SECONDS * eventCountToCapture - secondsDelta);
        query.clear();
        query.put(Event.untilParameterName, Arrays.asList(Long.toString(untilDateTime.getMillis())));
        results = searchIO.search(TENANT_RANGE, query);
        Assert.assertEquals(eventCountToCapture, results.size());

        query.clear();
        fromDateTime = new DateTime().minusSeconds(RANGE_STEP_IN_SECONDS * 2 - secondsDelta);
        untilDateTime = new DateTime().minusSeconds(RANGE_STEP_IN_SECONDS - secondsDelta);
        query.put(Event.fromParameterName, Arrays.asList(Long.toString(fromDateTime.getMillis())));
        query.put(Event.untilParameterName, Arrays.asList(Long.toString(untilDateTime.getMillis())));
        results = searchIO.search(TENANT_RANGE, query);
        Assert.assertEquals(1, results.size());
    }

    @BeforeClass
    public static void setup() throws Exception {
        esSetup = new EsSetup();
        esSetup.execute(EsSetup.deleteAll());
        esSetup.execute(EsSetup
                .createIndex(EventElasticSearchIO.EVENT_INDEX)
                .withMapping(EventElasticSearchIO.ES_TYPE, EsSetup.fromClassPath("events_mapping.json")));
        searchIO = new EventElasticSearchIO();

        createTestEvents(TENANT_1, TENANT_1_EVENTS_NUM);
        createTestEvents(TENANT_2, TENANT_2_EVENTS_NUM);
        createTestEvents(TENANT_WITH_SYMBOLS, TENANT_WITH_SYMBOLS_NUM);
        createRangeEvents(TENANT_RANGE, TENANT_RANGE_EVENTS_NUM, RANGE_STEP_IN_SECONDS);

        int statusCode = searchIO.elasticsearchRestHelper.refreshIndex(EventElasticSearchIO.EVENT_INDEX);
        if(statusCode != 200) {
            System.out.println(String.format("Refresh for %s failed with status code: %d",
                    EventElasticSearchIO.EVENT_INDEX, statusCode));
        }
    }

    private static void createTestEvents(final String tenant, int eventCount) throws Exception {
        Map<String, Object> eventMap;
        final DateTime date = new DateTime();
        for (int i=0; i<eventCount; i++) {
            Event event = new Event();
            event.setWhat(String.format("[%s] %s %d", tenant, "Event title sample", i));
            event.setWhen(date.getMillis());
            event.setData(String.format("[%s] %s %d", tenant, "Event data sample", i));
            event.setTags(String.format("[%s] %s %d", tenant, "Event tags sample", i));

            eventMap = event.toMap();
            searchIO.insert(tenant, eventMap);
        }
    }

    private static void createRangeEvents(String tenant, int eventCount, int stepInSeconds) throws Exception {
        Map<String, Object> eventMap;
        DateTime date = new DateTime();
        for (int i=0;i<eventCount; i++) {
            Event event = new Event();
            event.setWhat("1");
            event.setWhen(date.getMillis());
            event.setData("2");
            event.setTags("event");
            eventMap = event.toMap();

            date = date.minusSeconds(stepInSeconds);
            searchIO.insert(tenant, eventMap);
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
