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

import com.rackspacecloud.blueflood.utils.ElasticsearchTestServer;
import com.rackspacecloud.blueflood.types.Event;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

public class EventElasticSearchIOIntegrationTest {
    private static EventElasticSearchIO searchIO;

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
        ElasticsearchTestServer.getInstance().ensureStarted();
        ElasticsearchTestServer.getInstance().reset();
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
}
