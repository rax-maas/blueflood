package com.rackspacecloud.blueflood.dw.ingest.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.Map;

@Immutable
public class Timer {
    private final String name;
    private final Number count;
    private final Number rate;
    private final Number min;
    private final Number max;
    private final Number sum;
    private final Number avg;
    private final Number median;
    private final Number std;
    private final Map<String, Percentile> percentiles;
    private final Map<String, Number> histogram;
    private final String tenant;
    
    public Timer(
            @JsonProperty("name") String name,
            @JsonProperty("count") Number count,
            @JsonProperty("rate") Number rate,
            @JsonProperty("min") Number min,
            @JsonProperty("max") Number max,
            @JsonProperty("sum") Number sum,
            @JsonProperty("avg") Number avg,
            @JsonProperty("median") Number median,
            @JsonProperty("std") Number std,
            @JsonProperty("percentiles") Map<String, Percentile> percentiles,
            @JsonProperty("histogram") Map<String, Number> histogram,
            @JsonProperty("tenant") String tenant) {
        this.name = name;
        this.count = count;
        this.rate = rate;
        this.min = min;
        this.max = max;
        this.sum = sum;
        this.avg = avg;
        this.median = median;
        this.std = std;
        this.percentiles = percentiles;
        this.histogram = histogram;
        this.tenant = tenant;
    }
    
    @JsonProperty
    public String getName() { return name; }
    
    @JsonProperty
    public Number getCount() { return count; }
    
    @JsonProperty
    public Number getRate() { return rate; }
    
    @JsonProperty
    public Number getMin() { return min; }
    
    @JsonProperty
    public Number getMax() { return max; }
    
    @JsonProperty
    public Number getSum() { return sum; }
    
    @JsonProperty
    public Number getAvg() { return avg; }
    
    @JsonProperty
    public Number getMedian() { return median; }
    
    @JsonProperty
    public Number getStd() { return std; }
    
    @JsonProperty
    public Map<String, Percentile> getPercentiles() { return Bundle.safeUnmodifiableMap(percentiles); }
    
    @JsonProperty
    public Map<String, Number> getHistogram() { return Bundle.safeUnmodifiableMap(histogram); }
    
    @JsonProperty
    public String getTenant() { return tenant; }
}
