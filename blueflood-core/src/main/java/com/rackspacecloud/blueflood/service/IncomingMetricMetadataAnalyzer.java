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

package com.rackspacecloud.blueflood.service;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.rackspacecloud.blueflood.exceptions.CacheException;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.exceptions.IncomingMetricException;
import com.rackspacecloud.blueflood.exceptions.IncomingTypeException;
import com.rackspacecloud.blueflood.exceptions.IncomingUnitException;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class IncomingMetricMetadataAnalyzer {
    private static final Logger log = LoggerFactory.getLogger(IncomingMetricMetadataAnalyzer.class);
    private static Timer scanMetricsTimer = Metrics.timer(IncomingMetricMetadataAnalyzer.class, "Scan meta for metrics");
    private static Timer checkMetaTimer = Metrics.timer(IncomingMetricMetadataAnalyzer.class, "Check meta");
    private static Configuration config = Configuration.getInstance();
    private static boolean USE_ES_FOR_UNITS = false;
    private static boolean ES_MODULE_FOUND = false;

    private final MetadataCache cache;
    
    public IncomingMetricMetadataAnalyzer(MetadataCache cache) {
        this.cache = cache;
        USE_ES_FOR_UNITS = config.getBooleanProperty(CoreConfig.USE_ES_FOR_UNITS);
        ES_MODULE_FOUND = config.getListProperty(CoreConfig.DISCOVERY_MODULES).contains(Util.ElasticIOPath);
    }
    
    public Collection<IncomingMetricException> scanMetrics(Collection<IMetric> metrics) {
        List<IncomingMetricException> problems = new ArrayList<IncomingMetricException>();

        Timer.Context ctx = scanMetricsTimer.time();
        for (IMetric metric : metrics) {
            try {
                if (metric instanceof Metric) {
                    Collection<IncomingMetricException> metricProblems = checkMetric((Metric) metric);
                    if (metricProblems != null) {
                        problems.addAll(metricProblems);
                    }
                }
            } catch (CacheException ex) {
                log.warn(ex.getMessage(), ex);
            }
        }
        ctx.stop();

        return problems;
    }

    private IncomingMetricException checkMeta(Locator locator, String key, String incoming) throws CacheException {
        Timer.Context ctx = checkMetaTimer.time();
        try {
            String existing = cache.get(locator, key, String.class);

            // always update the cache. it is smart enough to avoid needless writes.
            cache.put(locator, key, incoming);

            boolean differs = existing != null && !incoming.equals(existing);
            if (differs) {
                if (key.equals(MetricMetadata.UNIT.name().toLowerCase())) {
                    return new IncomingUnitException(locator, existing, incoming);
                } else {
                    return new IncomingTypeException(locator, existing, incoming);
                }
            }
        } finally {
            ctx.stop();
        }

        return null;
    }

    private Collection<IncomingMetricException> checkMetric(Metric metric) throws CacheException {
        if (metric == null) {
            return null;
        }

        List<IncomingMetricException> problems = new ArrayList<IncomingMetricException>();
        IncomingMetricException typeProblem = null;

        if (metric.getDataType() != DataType.NUMERIC) {
            typeProblem = checkMeta(metric.getLocator(), MetricMetadata.TYPE.name().toLowerCase(),
                    metric.getDataType().toString());
        }

        if (typeProblem != null) {
            problems.add(typeProblem);
        }

        if (!USE_ES_FOR_UNITS || !ES_MODULE_FOUND) {
            if (USE_ES_FOR_UNITS && !ES_MODULE_FOUND) {
                log.warn("USE_ES_FOR_UNITS config found but ES discovery module not found in the config, will use the metadata cache for units");
            }
            IncomingMetricException unitProblem = checkMeta(metric.getLocator(), MetricMetadata.UNIT.name().toLowerCase(),
                    metric.getUnit());
            if (unitProblem != null) {
                problems.add(unitProblem);
            }
        }
        return problems;
    }

    @VisibleForTesting
    public static void setEsForUnits(boolean setEsForUnits) {
        USE_ES_FOR_UNITS = setEsForUnits;
    }

    @VisibleForTesting
    public static void setEsModuleFoundForUnits(boolean setEsModuleFound) {
        ES_MODULE_FOUND = setEsModuleFound;
    }
}
