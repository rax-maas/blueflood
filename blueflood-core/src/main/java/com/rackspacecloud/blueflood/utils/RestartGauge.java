package com.rackspacecloud.blueflood.utils;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricsRegistry;

public class RestartGauge extends Gauge<Integer> {
    boolean sentVal = false;

    @Override
    public Integer value() {
        // Sends a value of 1 for the first flush after service restart and Gauge instantiation, then zero.
        if (!sentVal){
            sentVal = true;
            return 1;
        }
        return 0;
    }

    public RestartGauge(MetricsRegistry registry, Class klass) {
        registry.newGauge(klass, "Restart", this);
    }

    public RestartGauge(Class klass) {
        this(Metrics.defaultRegistry(), klass);
    }

}