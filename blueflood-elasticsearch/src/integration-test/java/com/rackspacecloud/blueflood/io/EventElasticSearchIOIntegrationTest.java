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

import com.rackspacecloud.blueflood.types.Event;
import com.rackspacecloud.blueflood.types.IMetric;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;


/**
 * The current scope gives us one cluster for all test methods in the test.
 * All indices and templates are deleted between each test.
 *
 * The following flags have to be set while running this test
 * -Dtests.jarhell.check=false (to handle some bug in intellij https://github.com/elastic/elasticsearch/issues/14348)
 * -Dtests.security.manager=false (https://github.com/elastic/elasticsearch/issues/16459)
 *
 */
public class EventElasticSearchIOIntegrationTest extends BaseElasticTest {
    private EventElasticSearchIO searchIO;

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
        Map<String, List<String>> query = new HashMap<String, List<String>>();
        query.put(Event.tagsParameterName, Arrays.asList("event"));
        List<Map<String, Object>> results = searchIO.search(TENANT_1, query);
        assertEquals(TENANT_1_EVENTS_NUM, results.size());

        results = searchIO.search(TENANT_2, query);
        assertEquals(TENANT_2_EVENTS_NUM, results.size());

        results = searchIO.search(TENANT_RANGE, query);
        assertEquals(TENANT_RANGE_EVENTS_NUM, results.size());

        results = searchIO.search(TENANT_WITH_SYMBOLS, query);
        assertEquals(TENANT_WITH_SYMBOLS_NUM, results.size());
    }

    @Test
    public void testEmptyQueryParameters() throws Exception {
        Map<String, List<String>> query = new HashMap<String, List<String>>();
        query.put(Event.tagsParameterName, new ArrayList<String>());
        query.put(Event.fromParameterName, new ArrayList<String>());
        query.put(Event.untilParameterName, new ArrayList<String>());

        List<Map<String, Object>> results = searchIO.search(TENANT_1, query);
        assertEquals(TENANT_1_EVENTS_NUM, results.size());
    }

    @Test
    public void testEventTagsOnlySearch() throws Exception {
        Map<String, List<String>> query = new HashMap<String, List<String>>();
        query.put(Event.tagsParameterName, Arrays.asList("sample"));
        List<Map<String, Object>> results = searchIO.search(TENANT_1, query);
        assertEquals(TENANT_1_EVENTS_NUM, results.size());

        query.put(Event.tagsParameterName, Arrays.asList("1"));
        results = searchIO.search(TENANT_1, query);
        assertEquals(1, results.size());

        query.put(Event.tagsParameterName, Arrays.asList("database"));
        results = searchIO.search(TENANT_1, query);
        assertEquals(0, results.size());
    }

    @Test
    public void testEmptyQuery() throws Exception {
        List<Map<String, Object>> results = searchIO.search(TENANT_1, null);
        assertEquals(TENANT_1_EVENTS_NUM, results.size());
    }

    @Test
    public void testRangeOnlySearch() throws Exception {
        Map<String, List<String>> query = new HashMap<String, List<String>>();
        final int eventCountToCapture = TENANT_RANGE_EVENTS_NUM / 2;
        final int secondsDelta = 10;
        DateTime fromDateTime = new DateTime().minusSeconds(RANGE_STEP_IN_SECONDS * eventCountToCapture - secondsDelta);
        query.put(Event.fromParameterName, Arrays.asList(Long.toString(fromDateTime.getMillis())));
        List<Map<String, Object>> results = searchIO.search(TENANT_RANGE, query);
        assertEquals(eventCountToCapture, results.size());

        DateTime untilDateTime = new DateTime().minusSeconds(RANGE_STEP_IN_SECONDS * eventCountToCapture - secondsDelta);
        query.clear();
        query.put(Event.untilParameterName, Arrays.asList(Long.toString(untilDateTime.getMillis())));
        results = searchIO.search(TENANT_RANGE, query);
        assertEquals(eventCountToCapture, results.size());

        query.clear();
        fromDateTime = new DateTime().minusSeconds(RANGE_STEP_IN_SECONDS * 2 - secondsDelta);
        untilDateTime = new DateTime().minusSeconds(RANGE_STEP_IN_SECONDS - secondsDelta);
        query.put(Event.fromParameterName, Arrays.asList(Long.toString(fromDateTime.getMillis())));
        query.put(Event.untilParameterName, Arrays.asList(Long.toString(untilDateTime.getMillis())));
        results = searchIO.search(TENANT_RANGE, query);
        assertEquals(1, results.size());
    }

    @Before
    public void setup() throws Exception {

        createIndexAndMapping(EventElasticSearchIO.EVENT_INDEX, EventElasticSearchIO.ES_TYPE, getEventsMapping());

        searchIO = new EventElasticSearchIO(getClient());

        createTestEvents(TENANT_1, TENANT_1_EVENTS_NUM);
        createTestEvents(TENANT_2, TENANT_2_EVENTS_NUM);
        createTestEvents(TENANT_WITH_SYMBOLS, TENANT_WITH_SYMBOLS_NUM);
        createRangeEvents(TENANT_RANGE, TENANT_RANGE_EVENTS_NUM, RANGE_STEP_IN_SECONDS);

        refreshChanges();
    }

    private void createTestEvents(final String tenant, int eventCount) throws Exception {
        ArrayList<Map<String, Object>> eventList = new ArrayList<Map<String, Object>>();
        final DateTime date = new DateTime();
        for (int i=0; i<eventCount; i++) {
            Event event = new Event();
            event.setWhat(String.format("[%s] %s %d", tenant, "Event title sample", i));
            event.setWhen(date.getMillis());
            event.setData(String.format("[%s] %s %d", tenant, "Event data sample", i));
            event.setTags(String.format("[%s] %s %d", tenant, "Event tags sample", i));

            eventList.add(event.toMap());
        }

        searchIO.insert(tenant, eventList);
    }

    private void createRangeEvents(String tenant, int eventCount, int stepInSeconds) throws Exception {
        ArrayList<Map<String, Object>> eventList = new ArrayList<Map<String, Object>>();
        DateTime date = new DateTime();
        for (int i=0;i<eventCount; i++) {
            Event event = new Event();
            event.setWhat("1");
            event.setWhen(date.getMillis());
            event.setData("2");
            event.setTags("event");
            eventList.add(event.toMap());

            date = date.minusSeconds(stepInSeconds);
        }
        searchIO.insert(tenant, eventList);
    }

    @Override
    protected void insertDiscovery(List<IMetric> metrics) throws IOException {
        //dummy implmentation. We dont need to insert any metrics as we are only inserting events.
    }
}
