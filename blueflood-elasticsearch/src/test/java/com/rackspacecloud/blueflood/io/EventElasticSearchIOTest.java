package com.rackspacecloud.blueflood.io;

import com.github.tlrx.elasticsearch.test.EsSetup;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class EventElasticSearchIOTest {
    private EventElasticSearchIO searchIO;
    private EsSetup esSetup;

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
        query.put("tags", Arrays.asList("event"));
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
        Map<String, List<String>> query = new HashMap<String, List<String>>();
        query.put("tags", new ArrayList<String>());
        query.put("from", new ArrayList<String>());
        query.put("until", new ArrayList<String>());

        List<Map<String, Object>> results = searchIO.search(TENANT_1, query);
        Assert.assertEquals(TENANT_1_EVENTS_NUM, results.size());
    }



    @Test
    public void testEventTagsOnlySearch() throws Exception {
        Map<String, List<String>> query = new HashMap<String, List<String>>();
        query.put("tags", Arrays.asList("sample"));
        List<Map<String, Object>> results = searchIO.search(TENANT_1, query);
        Assert.assertEquals(TENANT_1_EVENTS_NUM, results.size());

        query.put("tags", Arrays.asList("1"));
        results = searchIO.search(TENANT_1, query);
        Assert.assertEquals(1, results.size());

        query.put("tags", Arrays.asList("database"));
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
        Map<String, List<String>> query = new HashMap<String, List<String>>();
        final int eventCountToCapture = TENANT_RANGE_EVENTS_NUM / 2;
        final int secondsDelta = 10;
        DateTime fromDateTime = new DateTime().minusSeconds(RANGE_STEP_IN_SECONDS * eventCountToCapture - secondsDelta);
        query.put("from", Arrays.asList(Long.toString(fromDateTime.getMillis() / 1000)));
        List<Map<String, Object>> results = searchIO.search(TENANT_RANGE, query);
        Assert.assertEquals(eventCountToCapture, results.size());

        DateTime untilDateTime = new DateTime().minusSeconds(RANGE_STEP_IN_SECONDS * eventCountToCapture - secondsDelta);
        query.clear();
        query.put("until", Arrays.asList(Long.toString(untilDateTime.getMillis() / 1000)));
        results = searchIO.search(TENANT_RANGE, query);
        Assert.assertEquals(eventCountToCapture, results.size());

        query.clear();
        fromDateTime = new DateTime().minusSeconds(RANGE_STEP_IN_SECONDS * 2 - secondsDelta);
        untilDateTime = new DateTime().minusSeconds(RANGE_STEP_IN_SECONDS - secondsDelta);
        query.put("from", Arrays.asList(Long.toString(fromDateTime.getMillis() / 1000)));
        query.put("until", Arrays.asList(Long.toString(untilDateTime.getMillis() / 1000)));
        results = searchIO.search(TENANT_RANGE, query);
        Assert.assertEquals(1, results.size());
    }

    @Before
    public void setup() throws Exception {
        esSetup = new EsSetup();
        esSetup.execute(EsSetup.deleteAll());
        esSetup.execute(EsSetup
                .createIndex(EventElasticSearchIO.EVENT_INDEX)
                .withMapping(EventElasticSearchIO.ES_TYPE, EsSetup.fromClassPath("events_mapping.json")));
        searchIO = new EventElasticSearchIO(esSetup.client());

        createTestEvents(TENANT_1, TENANT_1_EVENTS_NUM);
        createTestEvents(TENANT_2, TENANT_2_EVENTS_NUM);
        createTestEvents(TENANT_WITH_SYMBOLS, TENANT_WITH_SYMBOLS_NUM);
        createRangeEvents(TENANT_RANGE, TENANT_RANGE_EVENTS_NUM, RANGE_STEP_IN_SECONDS);

        esSetup.client().admin().indices().prepareRefresh().execute().actionGet();
    }

    private void createTestEvents(String tenant, int eventCount) throws Exception {
        ArrayList<Map<String, Object>> eventList = new ArrayList<Map<String, Object>>();
        DateTime date = new DateTime();
        for (int i=0; i<eventCount; i++) {
            Map<String, Object> event = new HashMap<String, Object>();
            event.put("what", String.format("[%s] %s %d", tenant, "Event title sample", i));
            event.put("when", date.getMillis() / 1000);
            event.put("data", String.format("[%s] %s %d", tenant, "Event data sample", i));
            event.put("tags", String.format("[%s] %s %d", tenant, "Event tags sample", i));
            eventList.add(event);
        }

        searchIO.insert(tenant, eventList);
    }

    private void createRangeEvents(String tenant, int eventCount, int stepInSeconds) throws Exception {
        ArrayList<Map<String, Object>> eventList = new ArrayList<Map<String, Object>>();
        DateTime date = new DateTime();
        for (int i=0;i<eventCount; i++) {
            Map<String, Object> event = new HashMap<String, Object>();
            event.put("what", "1");
            event.put("when", date.getMillis() / 1000);
            event.put("data", "2");
            event.put("tags", "event");
            eventList.add(event);

            date = date.minusSeconds(stepInSeconds);
        }
        searchIO.insert(tenant, eventList);
    }

    @After
    public void tearDown() {
        esSetup.terminate();
    }
}
