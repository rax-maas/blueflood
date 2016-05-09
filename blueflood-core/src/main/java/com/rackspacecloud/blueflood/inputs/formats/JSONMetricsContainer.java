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

package com.rackspacecloud.blueflood.inputs.formats;

import com.rackspacecloud.blueflood.io.Instrumentation;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class JSONMetricsContainer {
    private final String tenantId;
    private final List<JSONMetric> jsonMetrics;

    private static final long TRACKER_DELAYED_METRICS_MILLIS = Configuration.getInstance().getLongProperty(CoreConfig.TRACKER_DELAYED_METRICS_MILLIS);
    private static final long MAX_AGE_ALLOWED = Configuration.getInstance().getLongProperty(CoreConfig.ROLLUP_DELAY_MILLIS);
    private static final long SHORT_DELAY = Configuration.getInstance().getLongProperty(CoreConfig.SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS);

    private static final long pastDiff = Configuration.getInstance().getLongProperty( CoreConfig.BEFORE_CURRENT_COLLECTIONTIME_MS );
    private static final long futureDiff = Configuration.getInstance().getLongProperty( CoreConfig.AFTER_CURRENT_COLLECTIONTIME_MS );

    private final List<Metric> metrics = new ArrayList<Metric>();
    private final List<Metric> delayedMetrics = new ArrayList<Metric>();
    private final List<String> errors = new ArrayList<String>();

    public JSONMetricsContainer(String tenantId, List<JSONMetric> metrics) {
        this.tenantId = tenantId;
        this.jsonMetrics = metrics;
        processJson();
    }

    public List<String> getValidationErrors() {
        return errors;
    }

    public List<Metric> getValidMetrics() {
        return metrics;
    }

    private List<Metric> processJson() {

        if (jsonMetrics == null || jsonMetrics.isEmpty()) {
            return null;
        }

        for (JSONMetric jsonMetric : jsonMetrics) {

            // validate metric and retrieve error message if failed
            List<String> metricValidationErrors = jsonMetric.getValidationErrors();
            if ( !metricValidationErrors.isEmpty() ) {
                // has metric has validation errors, do not convert metric and add to errors list and go to next metric
                errors.addAll(metricValidationErrors);
                continue;
            }

            if (jsonMetric.getMetricValue() == null) {
                // skip null value
                continue;
            }

            // no error, create metric from json values, but skip null metricValue
            Locator locator;
            if (jsonMetric instanceof JSONMetricScoped) {
                JSONMetricScoped scopedMetric = (JSONMetricScoped)jsonMetric;
                locator = Locator.createLocatorFromPathComponents(scopedMetric.getTenantId(), jsonMetric.getMetricName());
            } else {
                locator = Locator.createLocatorFromPathComponents(tenantId, jsonMetric.getMetricName());
            }

            final Metric metric = new Metric(locator, jsonMetric.getMetricValue(), jsonMetric.getCollectionTime(),
                    new TimeValue(jsonMetric.getTtlInSeconds(), TimeUnit.SECONDS), jsonMetric.getUnit());
            long delay = new DateTime().getMillis() - metric.getCollectionTime();

            if (delay > TRACKER_DELAYED_METRICS_MILLIS) {
                delayedMetrics.add(metric);
            }

            if (delay > MAX_AGE_ALLOWED) {
                if (delay <= SHORT_DELAY) {
                    Instrumentation.markMetricsWithShortDelayReceived();
                } else {
                    Instrumentation.markMetricsWithLongDelayReceived();
                }
            }

            metrics.add(metric);
        }

        return metrics;
    }

    public boolean areDelayedMetricsPresent() {
        return delayedMetrics.size() > 0;
    }

    public List<Metric> getDelayedMetrics() {
        return delayedMetrics;
    }

}
