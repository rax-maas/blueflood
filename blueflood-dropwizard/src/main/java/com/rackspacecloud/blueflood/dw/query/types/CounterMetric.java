package com.rackspacecloud.blueflood.dw.query.types;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonFilter("dynamic")
public class CounterMetric extends Metric {
    private static final String COUNT = "count";
    private static final String RATE = "rate";
    private static final String SAMPLES = "samples";
    
    @JsonProperty(COUNT)
    public void setCount(Number number) {
        this.put(COUNT, number);
    }
    
    @JsonProperty(COUNT)
    public Number getCount() {
        return this.get(COUNT);
    }
    
    @JsonProperty(RATE)
    public void setRate(Number number) {
        this.put(RATE, number);
    }
    
    @JsonProperty(RATE)
    public Number getRate() {
        return this.get(RATE);
    }
    
    @JsonProperty(SAMPLES)
    public void setSamples(Number number) {
        this.put(SAMPLES, number);
    }
    
    @JsonProperty(SAMPLES)
    public Number getSamples() {
        return this.get(SAMPLES);
    }
}
