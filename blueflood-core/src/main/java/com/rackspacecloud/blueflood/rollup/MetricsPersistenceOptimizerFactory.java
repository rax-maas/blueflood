package com.rackspacecloud.blueflood.rollup;

import com.rackspacecloud.blueflood.types.Metric;

import java.util.HashMap;
import java.util.Map;

public class MetricsPersistenceOptimizerFactory {
    private enum MetricType {
        STRING, STATUS, GENERIC
    }

    private static final Map<MetricType, MetricsPersistenceOptimizer>
            optimizers =
            new HashMap<MetricType, MetricsPersistenceOptimizer>() {{
                MetricsPersistenceOptimizer stringType = new
                        StringMetricsPersistenceOptimizer();
                put(MetricType.STRING, stringType);

                // state metrics also are like string metrics (for now)
                put(MetricType.STATUS, stringType);

                MetricsPersistenceOptimizer genericType = new
                        GenericMetricsPersistenceOptimizer();
                put(MetricType.GENERIC, genericType);
            }};

    private MetricsPersistenceOptimizerFactory() {
        // left empty
    }

    // get the right optimizer based on metric type
    public static MetricsPersistenceOptimizer getOptimizer(Metric.Type metricType) {
        if (metricType == Metric.Type.STRING || metricType == Metric.Type.BOOLEAN) {
            return optimizers.get(MetricType.STRING);
        } else {
            return optimizers.get(MetricType.GENERIC);
        }
    }
}