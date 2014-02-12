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

import com.rackspacecloud.blueflood.exceptions.CacheException;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.exceptions.IncomingMetricException;
import com.rackspacecloud.blueflood.exceptions.IncomingTypeException;
import com.rackspacecloud.blueflood.exceptions.IncomingUnitException;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.MetricMetadata;
import com.rackspacecloud.blueflood.types.Locator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class IncomingMetricMetadataAnalyzer {
    private static final Logger log = LoggerFactory.getLogger(IncomingMetricMetadataAnalyzer.class);
    
    private final MetadataCache cache;
    
    public IncomingMetricMetadataAnalyzer(MetadataCache cache) {
        this.cache = cache;
    }
    
    public Collection<IncomingMetricException> scanMetrics(Collection<IMetric> metrics) {
        List<IncomingMetricException> problems = new ArrayList<IncomingMetricException>();
        for (IMetric metric : metrics) {
            try {
                if (metric instanceof Metric) {
                    Collection<IncomingMetricException> metricProblems = checkMetric((Metric)metric);
                    if (metricProblems != null) {
                        problems.addAll(metricProblems);
                    }
                }
            } catch (CacheException ex) {
                log.warn(ex.getMessage(), ex);
            }
        }
        return problems;
    }

    private IncomingMetricException checkMeta(Locator locator, String key, String incoming) throws CacheException {
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

        return null;
    }

    private Collection<IncomingMetricException> checkMetric(Metric metric) throws CacheException {
        if (metric == null) {
            return null;
        }

        List<IncomingMetricException> problems = new ArrayList<IncomingMetricException>();

        IncomingMetricException typeProblem = checkMeta(metric.getLocator(), MetricMetadata.TYPE.name().toLowerCase(),
                metric.getType().getCode());
        IncomingMetricException unitProblem = checkMeta(metric.getLocator(), MetricMetadata.UNIT.name().toLowerCase(),
                metric.getUnit());

        if (typeProblem != null) {
            problems.add(typeProblem);
        }
        if (unitProblem != null) {
            problems.add(unitProblem);
        }

        return problems;
    }
}
