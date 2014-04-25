package com.rackspacecloud.blueflood.dw.ingest.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;
import java.util.Arrays;

@Immutable
public class Set {
    private final String name;
    private final String[] values;
    private final String tenant;
    
    public Set(@JsonProperty("name") String name, @JsonProperty("values") String[] values, @JsonProperty("tenant") String tenant) {
        this.name = name;
        this.values = values;
        this.tenant = tenant;
    }
    
    @JsonProperty
    public String getName() { return name; }
    
    @JsonProperty
    public String[] getValues() { return Arrays.copyOf(values, values.length, String[].class); };
    
    @JsonProperty
    public String getTenant() { return tenant; }
}
