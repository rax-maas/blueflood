package com.rackspacecloud.blueflood.dw.ingest.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

@Immutable
public class Counter {
    private final String name;
    private final Number value;
    private final Number rate;
    private final String tenant;
    
    public Counter(@JsonProperty("name") String name, @JsonProperty("value") Number value, @JsonProperty("rate") Number rate, @JsonProperty("tenant") String tenant) {
        this.name = name;
        this.value = value;
        this.rate = rate;
        this.tenant = tenant;
    }
    
    @JsonProperty
    public String getName() { return name; }
    
    @JsonProperty
    public Number getValue() { return value; }
    
    @JsonProperty
    public Number getRate() { return rate; }
    
    @JsonProperty
    public String getTenant() { return tenant; }
}
