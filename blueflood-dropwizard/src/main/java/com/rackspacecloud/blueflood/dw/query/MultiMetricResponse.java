package com.rackspacecloud.blueflood.dw.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rackspacecloud.blueflood.dw.query.types.Metric;
import com.rackspacecloud.blueflood.dw.query.types.SingleMetricResponse;

import java.util.HashMap;
import java.util.Map;

public class MultiMetricResponse {
    
    private Map<String, UnwrappedSingleMetricResponse> metrics = new HashMap<String, UnwrappedSingleMetricResponse>();
    
    public void addResponse(String metricName, SingleMetricResponse response) {
        metrics.put(metricName, new UnwrappedSingleMetricResponse(response));
    }
    
    @JsonProperty("metrics")
    public Map<String, UnwrappedSingleMetricResponse> getMetrics() {
        // could return a unmodifiable, but hey. optimize prematurely.
        return metrics;
    }
    
    public static class UnwrappedSingleMetricResponse {
        private final SingleMetricResponse response;
        
        public UnwrappedSingleMetricResponse(SingleMetricResponse r) {
            response = r;
        }
        
        @JsonProperty("values")
        public Metric[] getMetrics() { return response.getMetrics(); }
        
        @JsonProperty("type")
        public String getType() { return response.getType(); }
    }
}
