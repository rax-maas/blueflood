package com.rackspacecloud.blueflood.rollup;

import com.rackspacecloud.blueflood.types.Metric;

/**
 * This class is responsible for figuring out if an incoming metric
 * of generic type needs to be persisted or not
 */
public class GenericMetricsPersistenceOptimizer implements
        MetricsPersistenceOptimizer {
    public GenericMetricsPersistenceOptimizer() {
        // left empty
    }

    @Override
    public boolean shouldPersist(Metric metric) throws Exception {
        return true;
    }
}