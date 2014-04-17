package com.rackspacecloud.blueflood.dw.query.types;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonFilter("dynamic")
public class SetMetric extends Metric {
    public static final String COUNT = "count";
    
    @JsonProperty(COUNT)
    public void setDistinctValues(int count) {
        this.put(COUNT, count);
    }
    
    @JsonProperty(COUNT)
    public int getDistinctValues() {
        return this.get(COUNT);
    }
}
