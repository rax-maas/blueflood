package com.cloudkick.blueflood.internal;

import com.cloudkick.blueflood.utils.TimeValue;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/** expose more bits as they are needed. */
public class Account {
    private static final Gson gson;
    
    private String id;
    private String external_id;
    private Map<String, Integer> metrics_ttl;
    
    static {
        gson = new GsonBuilder().serializeNulls().create();
    }
    
    public static Account fromJSON(String json) {
        return gson.fromJson(json, Account.class);
    }
    
    public TimeValue getMetricTtl(String resolution) {
        return new TimeValue(metrics_ttl.get(resolution), TimeUnit.HOURS);
    }
    
    public String getId() { return id; };
    
    public String getTenantId() { return external_id; };
     
}
