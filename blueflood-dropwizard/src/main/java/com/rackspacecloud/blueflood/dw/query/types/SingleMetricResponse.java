package com.rackspacecloud.blueflood.dw.query.types;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SingleMetricResponse {
    
    private Metric[] metrics;
    
    private Paging paging;

    @JsonProperty("values")
    public Metric[] getMetrics() { return metrics; }

    @JsonProperty("values")
    public void setMetrics(Metric[] metrics) { this.metrics = metrics; }

    @JsonProperty("metadata")
    public Paging getPaging() { return paging; }

    @JsonProperty("metadata")
    public void setPaging(Paging paging) { this.paging = paging; }
}
