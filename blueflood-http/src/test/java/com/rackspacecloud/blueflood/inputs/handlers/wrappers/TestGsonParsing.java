package com.rackspacecloud.blueflood.inputs.handlers.wrappers;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.rackspacecloud.blueflood.inputs.handlers.HttpStatsDIngestionHandler;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class TestGsonParsing {
    
    private String json;
    
    @Before
    public void readJsonFile() throws IOException {
        StringBuilder sb = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("src/test/resources/sample_bundle.json")));
        String curLine = reader.readLine();
        while (curLine != null) {
            sb = sb.append(curLine);
            curLine = reader.readLine();
        }
        json = sb.toString();
    }
    
    @Test
    public void testLameButValidJSON() {
        String badJson = "{}";
        Bundle bundle = HttpStatsDIngestionHandler.createBundle(badJson); 
    }
    
    @Test(expected = JsonSyntaxException.class)
    public void testInvalidJSON() {
        String badJson = "{tenantId:}";
        Bundle bundle = HttpStatsDIngestionHandler.createBundle(badJson);
    }
    
    @Test
    public void testBasicBundle() {
        Gson gson = new Gson();
        Bundle bundle = gson.fromJson(json, Bundle.class);

        Assert.assertNotNull(bundle);
        Assert.assertEquals("333333", bundle.getTenantId());
        Assert.assertEquals(1389211230L, bundle.getTimestamp());
        Assert.assertEquals(15000L, bundle.getFlushIntervalMillis());
        
        Assert.assertEquals(4, bundle.getGauges().size());
        Assert.assertEquals(6, bundle.getCounters().size());
        Assert.assertEquals(4, bundle.getTimers().size());
        Assert.assertEquals(2, bundle.getSets().size());
        Assert.assertEquals(1, bundle.getEnums().size());
    }

    @Test
    public void testHistograms() {
        Bundle bundle = new Gson().fromJson(json, Bundle.class);
        
        Assert.assertNotNull(bundle);
        Map<String, Bundle.Timer> timers = asMap(bundle.getTimers());
        
        Assert.assertEquals(4, timers.get("4444444.T1s").getHistogram().size());
        Assert.assertEquals(11, timers.get("3333333.T29s").getHistogram().size());
        Assert.assertEquals(11, timers.get("3333333.T200ms").getHistogram().size());
        
        // this one is non-existant in the json, but we do not want a null map.
        Assert.assertNotNull(timers.get("3333333.T10s").getHistogram());
        Assert.assertEquals(0, timers.get("3333333.T10s").getHistogram().size());
    }
    
    @Test
    public void testPercentiles() {
        Bundle bundle = new Gson().fromJson(json, Bundle.class);
        
        Assert.assertNotNull(bundle);
        Map<String, Bundle.Timer> timers = asMap(bundle.getTimers());
        
        Assert.assertEquals(5, timers.get("4444444.T1s").getPercentiles().size());
        Assert.assertEquals(5, timers.get("3333333.T29s").getPercentiles().size());
        Assert.assertEquals(5, timers.get("3333333.T10s").getPercentiles().size());
        Assert.assertEquals(5, timers.get("3333333.T200ms").getPercentiles().size());
    }
    
    private static Map<String, Bundle.Timer> asMap(Collection<Bundle.Timer> timers) {
        Map<String, Bundle.Timer> map = new HashMap<String, Bundle.Timer>(timers.size());
        for (Bundle.Timer timer : timers)
            map.put(timer.getName(), timer);
        return map;
    }
}
