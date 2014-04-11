package com.rackspacecloud.blueflood.dw.ingest.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

@Immutable
public class Percentile {
    private final Number avg;
    private final Number max;
    private final Number sum;
    
    public Percentile(@JsonProperty("avg") Number avg, @JsonProperty("max") Number max, @JsonProperty("sum") Number sum) {
        this.avg = avg;
        this.max = max;
        this.sum = sum;
    }
    
    @JsonProperty
    public Number getAvg() { return avg; }
    
    @JsonProperty
    public Number getMax() { return max; }
    
    @JsonProperty
    public Number getSum() { return sum; }
}
