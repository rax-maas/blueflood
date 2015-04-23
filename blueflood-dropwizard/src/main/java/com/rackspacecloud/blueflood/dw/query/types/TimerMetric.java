package com.rackspacecloud.blueflood.dw.query.types;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

@JsonFilter("dynamic")
public class TimerMetric extends BasicRollupMetric {
    private static final String SUM = "sum";
    private static final String RATE = "rate";
    private static final String SAMPLES = "samples";
    private static final String PERCENTILES = "percentiles";
    
    @JsonProperty(SUM)
    public void setSum(Number number) {
        this.put(SUM, number);
    }
    
    @JsonProperty(SUM)
    public Number getSum() {
        return this.get(SUM);
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
    
    @JsonProperty(PERCENTILES)
    public void setPercentiles(Map<String, Number> map) {
        this.put(PERCENTILES, map);
    }
    
    @JsonProperty(PERCENTILES)
    public Map<String, Number> getPercentiles() {
        return this.get(PERCENTILES);
    }
    
}
