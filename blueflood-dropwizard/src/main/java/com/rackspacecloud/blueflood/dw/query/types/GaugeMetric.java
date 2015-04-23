package com.rackspacecloud.blueflood.dw.query.types;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonFilter("dynamic")
public class GaugeMetric extends BasicRollupMetric {
    private static final String LATEST = "latest";
    
    @JsonProperty(LATEST)
    public void setLatest(Number n) {
        this.put(LATEST, n);
    }
    
    @JsonProperty(LATEST)
    public Number getLatest() {
        return this.get(LATEST);
    }
}
