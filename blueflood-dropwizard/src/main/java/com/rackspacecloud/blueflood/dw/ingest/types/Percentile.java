package com.rackspacecloud.blueflood.dw.ingest.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

@Immutable
public class Percentile {
    private final String label;
    private final Number value;

    public Percentile(@JsonProperty("label") String label, @JsonProperty("value") Number value) {
        this.label = label;
        this.value = value;
    }

    @JsonProperty
    public Number getValue() { return value; }
    
    @JsonProperty
    public String getLabel() { return label; }
}
