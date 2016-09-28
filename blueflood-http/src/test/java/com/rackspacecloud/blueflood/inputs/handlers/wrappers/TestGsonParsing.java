package com.rackspacecloud.blueflood.inputs.handlers.wrappers;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.rackspacecloud.blueflood.inputs.formats.AggregatedPayload;
import com.rackspacecloud.blueflood.types.BluefloodTimer;
import com.rackspacecloud.blueflood.inputs.handlers.HttpAggregatedIngestionHandler;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static com.rackspacecloud.blueflood.TestUtils.*;

public class TestGsonParsing {

    private String postfix = "postfix";

    private String json;
    private long current = System.currentTimeMillis();

    @Before
    public void readJsonFile() throws IOException, InterruptedException {
        json = getJsonFromFile("sample_payload.json", postfix);
    }
    
    @Test
    public void testLameButValidJSON() {
        String badJson = "{}";
        AggregatedPayload payload = AggregatedPayload.create(badJson);
    }
    
    @Test(expected = JsonSyntaxException.class)
    public void testInvalidJSON() {
        String badJson = "{tenantId:}";
        AggregatedPayload payload = AggregatedPayload.create(badJson);
    }
    
    @Test
    public void testBasicAggregatedPayload() {
        Gson gson = new Gson();
        AggregatedPayload payload = gson.fromJson(json, AggregatedPayload.class);

        assertNotNull(payload);
        assertEquals("333333", payload.getTenantId());
        assertEquals(current, payload.getTimestamp(), 120000 );
        assertEquals(15000L, payload.getFlushIntervalMillis());
        
        assertEquals(4, payload.getGauges().size());
        assertEquals(6, payload.getCounters().size());
        assertEquals(4, payload.getTimers().size());
        assertEquals(2, payload.getSets().size());
        assertEquals(1, payload.getEnums().size());
    }

    @Test
    public void testHistograms() {
        AggregatedPayload payload = new Gson().fromJson(json, AggregatedPayload.class);
        
        assertNotNull(payload);
        Map<String, BluefloodTimer> timers = asMap(payload.getTimers());
        
        assertEquals(4, timers.get( "4444444.T1s" + postfix ).getHistogram().size());
        assertEquals(11, timers.get( "3333333.T29s" + postfix ).getHistogram().size());
        assertEquals(11, timers.get( "3333333.T200ms" + postfix ).getHistogram().size());
        
        // this one is non-existant in the json, but we do not want a null map.
        assertNotNull(timers.get( "3333333.T10s" + postfix ).getHistogram());
        assertEquals(0, timers.get( "3333333.T10s" + postfix ).getHistogram().size());
    }
    
    @Test
    public void testPercentiles() {
        AggregatedPayload payload = new Gson().fromJson(json, AggregatedPayload.class);
        
        assertNotNull(payload);
        Map<String, BluefloodTimer> timers = asMap(payload.getTimers());
        
        assertEquals(5, timers.get( "4444444.T1s" + postfix ).getPercentiles().size());
        assertEquals(5, timers.get( "3333333.T29s" + postfix ).getPercentiles().size());
        assertEquals(5, timers.get( "3333333.T10s" + postfix ).getPercentiles().size());
        assertEquals(5, timers.get( "3333333.T200ms" + postfix ).getPercentiles().size());
    }
    
    private static Map<String, BluefloodTimer> asMap(Collection<BluefloodTimer> timers) {
        Map<String, BluefloodTimer> map = new HashMap<String, BluefloodTimer>(timers.size());
        for (BluefloodTimer timer : timers)
            map.put(timer.getName(), timer);
        return map;
    }
}
