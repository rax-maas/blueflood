package com.rackspacecloud.blueflood.dw.query.types;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rackspacecloud.blueflood.types.RollupType;

public class SingleMetricResponse {
    
    private Metric[] metrics;
    
    private String type;
    
    private Paging paging;

    @JsonProperty("values")
    public Metric[] getMetrics() { return metrics; }

    @JsonProperty("values")
    public void setMetrics(Metric[] metrics) { this.metrics = metrics; }

    @JsonProperty("metadata")
    public Paging getPaging() { return paging; }

    @JsonProperty("metadata")
    public void setPaging(Paging paging) { this.paging = paging; }
    
    @JsonProperty("type")
    public String getType() { return type; }
    
    @JsonProperty("type")
    public void setType(String s) {
        if (RollupType.BF_BASIC.name().toLowerCase().equals(s))
            type = "basic";
        else
            type = s; 
    }
}
