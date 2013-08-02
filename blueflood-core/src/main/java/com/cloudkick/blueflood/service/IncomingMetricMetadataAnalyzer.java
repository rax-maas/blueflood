package com.cloudkick.blueflood.service;

import com.cloudkick.blueflood.exceptions.CacheException;
import com.cloudkick.blueflood.cache.MetadataCache;
import com.cloudkick.blueflood.exceptions.IncomingMetricException;
import com.cloudkick.blueflood.exceptions.IncomingTypeException;
import com.cloudkick.blueflood.exceptions.IncomingUnitException;
import com.cloudkick.blueflood.types.Metric;
import com.cloudkick.blueflood.types.MetricMetadata;
import com.cloudkick.blueflood.types.Locator;
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
    
    public Collection<IncomingMetricException> scanMetrics(List<Metric> metrics) {
        List<IncomingMetricException> problems = new ArrayList<IncomingMetricException>();
        for (Metric metric : metrics) {
            try {
                Collection<IncomingMetricException> metricProblems = checkMetric(metric);
                if (metricProblems != null) {
                    problems.addAll(metricProblems);
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
                metric.getType().toString());
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
