package com.rackspacecloud.blueflood.dw.ingest.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

@Immutable
public class Gauge {
    
    private final String name;
    private final Number value;
    private final String tenant;
    
    public Gauge(@JsonProperty("name") String name, @JsonProperty("name") Number value, @JsonProperty("tenant") String tenant) {
        this.name = name;
        this.value = value;
        this.tenant = tenant;
    }
    
    @JsonProperty
    public String getName() { return name; }
    
    @JsonProperty
    public Number getValue() { return value; }
    
    @JsonProperty
    public String getTenant() { return tenant; }
}
