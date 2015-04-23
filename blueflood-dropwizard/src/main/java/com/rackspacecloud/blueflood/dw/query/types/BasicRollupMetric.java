package com.rackspacecloud.blueflood.dw.query.types;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonFilter("dynamic")
public class BasicRollupMetric extends Metric {
    private static final String AVERAGE = "average";
    private static final String MIN = "min";
    private static final String MAX = "max";
    private static final String VARIANCE = "variance";
    private static final String COUNT = "count";
    
    @JsonProperty(AVERAGE)
    public void setAverage(Number n) {
        this.put(AVERAGE, n);
    }
    
    @JsonProperty(AVERAGE)
    public Number getAverage() { 
        return get(AVERAGE); 
    }
    
    @JsonProperty(MIN)
    public void setMin(Number n) {
        this.put(MIN, n);
    }
    
    @JsonProperty(MIN)
    public Number getMin() { 
        return get(MIN); 
    }
    
    @JsonProperty(MAX)
    public void setMax(Number n) {
        this.put(MAX, n);
    }
    
    @JsonProperty(MAX)
    public Number getMax() { 
        return get(MAX); 
    }
    
    @JsonProperty(VARIANCE)
    public void setVariance(Number n) {
        this.put(VARIANCE, n);
    }
    
    @JsonProperty(VARIANCE)
    public Number getVariance() { 
        return get(VARIANCE); 
    }
    
    @JsonProperty(COUNT)
    public void setCount(Number n) {
        this.put(COUNT, n);
    }
    
    @JsonProperty(COUNT)
    public Number getCount() { 
        return get(COUNT); 
    }
}
