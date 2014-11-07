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

import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.Metric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for figuring out if an incoming String metric
 * needs to be persisted or not
 */
public class StringMetricsPersistenceOptimizer implements
        MetricsPersistenceOptimizer {
    private static final Logger log = LoggerFactory.getLogger(
            StringMetricsPersistenceOptimizer.class);
    private static boolean areStringMetricsDropped = Configuration.getInstance().getBooleanProperty(CoreConfig.STRING_METRICS_DROPPED);

    public StringMetricsPersistenceOptimizer() {
        // left empty
    }

    //to be called from tests
    public static void stringMetricsAreDropped() {
        areStringMetricsDropped = true;
    }

    //to be invoked only from tests
    public static void stringMetricsDroppedIsReset() {
        areStringMetricsDropped = Configuration.getInstance().getBooleanProperty(CoreConfig.STRING_METRICS_DROPPED);
    }

    @Override
    public boolean shouldPersist(Metric metric) throws Exception {
        if (areStringMetricsDropped) {
           return false;
        }
        else {
            String currentValue = String.valueOf(metric.getMetricValue());
            final String lastValue = AstyanaxReader.getInstance().getLastStringValue(metric.getLocator());
            return lastValue == null || !currentValue.equals(lastValue);
        }
    }
}
