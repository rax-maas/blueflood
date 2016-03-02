package com.rackspacecloud.blueflood.inputs.handlers.wrappers;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.rackspacecloud.blueflood.types.BluefloodTimer;
import com.rackspacecloud.blueflood.inputs.handlers.HttpAggregatedIngestionHandler;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TestGsonParsing {

    private static final String TIMESTAMP = "\"%TIMESTAMP%\"";
    private static final String PREFIX = "%PREFIX%";

    private String prefix = "prefix";

    public static String getJsonFromFile( Reader reader, String prefix ) throws IOException {

        return getJsonFromFile( reader, System.currentTimeMillis(), prefix );
    }

    public static String getJsonFromFile( Reader reader, long timestamp, String prefix ) throws IOException {
        StringWriter writer = new StringWriter();

        IOUtils.copy( reader, writer );
        IOUtils.closeQuietly( reader );

        String json = writer.toString();

        // JSON might have several entries for the same metric.  If they have the same timestamp, they coudl overwrite
        // each other in the case of enums.  Not using sleep() here to increment the time as to not have to deal with
        // interruptedexception.  Rather, incrementing the time by 1 ms.

        long increment = 0;

        while( json.contains( TIMESTAMP ) ) {

            json = json.replaceFirst( TIMESTAMP, Long.toString( timestamp + increment++ ) );
        }

        json = json.replace( PREFIX, prefix );

        return json;
    }

    private String json;
    private long current = System.currentTimeMillis();

    @Before
    public void readJsonFile() throws IOException, InterruptedException {

        json = getJsonFromFile( new InputStreamReader(new FileInputStream("src/test/resources/sample_payload.json")), prefix );
    }
    
    @Test
    public void testLameButValidJSON() {
        String badJson = "{}";
        AggregatedPayload payload = HttpAggregatedIngestionHandler.createPayload(badJson);
    }
    
    @Test(expected = JsonSyntaxException.class)
    public void testInvalidJSON() {
        String badJson = "{tenantId:}";
        AggregatedPayload payload = HttpAggregatedIngestionHandler.createPayload(badJson);
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
        
        assertEquals(4, timers.get( prefix + "4444444.T1s").getHistogram().size());
        assertEquals(11, timers.get( prefix + "3333333.T29s").getHistogram().size());
        assertEquals(11, timers.get( prefix + "3333333.T200ms").getHistogram().size());
        
        // this one is non-existant in the json, but we do not want a null map.
        assertNotNull(timers.get( prefix + "3333333.T10s").getHistogram());
        assertEquals(0, timers.get( prefix + "3333333.T10s").getHistogram().size());
    }
    
    @Test
    public void testPercentiles() {
        AggregatedPayload payload = new Gson().fromJson(json, AggregatedPayload.class);
        
        assertNotNull(payload);
        Map<String, BluefloodTimer> timers = asMap(payload.getTimers());
        
        assertEquals(5, timers.get( prefix + "4444444.T1s").getPercentiles().size());
        assertEquals(5, timers.get( prefix + "3333333.T29s").getPercentiles().size());
        assertEquals(5, timers.get( prefix + "3333333.T10s").getPercentiles().size());
        assertEquals(5, timers.get( prefix + "3333333.T200ms").getPercentiles().size());
    }
    
    private static Map<String, BluefloodTimer> asMap(Collection<BluefloodTimer> timers) {
        Map<String, BluefloodTimer> map = new HashMap<String, BluefloodTimer>(timers.size());
        for (BluefloodTimer timer : timers)
            map.put(timer.getName(), timer);
        return map;
    }
}
