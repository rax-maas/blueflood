/*
 * Copyright 2013 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.rollup;

import com.rackspacecloud.blueflood.types.DataType;
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
    public static MetricsPersistenceOptimizer getOptimizer(DataType metricType) {
        if (metricType == DataType.STRING || metricType == DataType.BOOLEAN) {
            return optimizers.get(MetricType.STRING);
        } else {
            return optimizers.get(MetricType.GENERIC);
        }
    }
}