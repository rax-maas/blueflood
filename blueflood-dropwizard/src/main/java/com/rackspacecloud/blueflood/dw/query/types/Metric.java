package com.rackspacecloud.blueflood.dw.query.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

public class Metric {
    private long timestamp;
    private String name;
    private Map<String, Object> attributes = new HashMap<String, Object>();
    
    public Metric() {
    }

    @JsonProperty
    public String getName() { return name; }

    @JsonProperty
    public void setName(String name) { this.name = name; }

    @JsonProperty
    public long getTimestamp() { return timestamp; }

    @JsonProperty
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    
    protected final void put(String key, Object value) {
        attributes.put(key, value);
    }
    
    protected final <T> T get(String key) {
        return (T)attributes.get(key);
    }
}
