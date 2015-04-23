package com.rackspacecloud.blueflood.dw.query.types;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonFilter("dynamic")
public class Numeric extends Metric {
    private static final String VALUE = "value";

    public Numeric() {
        super();
    }
    
    @JsonProperty(VALUE)
    public void setValue(Number value) {
        this.put(VALUE, value);
    }
    
    @JsonProperty(VALUE)
    public Number getValue() {
        return (Number)this.get(VALUE);
    }
}
