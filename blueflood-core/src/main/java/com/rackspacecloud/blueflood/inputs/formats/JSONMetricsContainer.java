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
import com.rackspacecloud.blueflood.outputs.formats.ErrorResponse;
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

    private static final long TRACKER_DELAYED_METRICS_MILLIS = Configuration.getInstance().getLongProperty(CoreConfig.TRACKER_DELAYED_METRICS_MILLIS);
    private static final long MAX_AGE_ALLOWED = Configuration.getInstance().getLongProperty(CoreConfig.ROLLUP_DELAY_MILLIS);
    private static final long SHORT_DELAY = Configuration.getInstance().getLongProperty(CoreConfig.SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS);

    private final String tenantId;

    private final List<Metric> validMetrics;
    private final List<ErrorResponse.ErrorData> validationErrors;
    private final List<Metric> delayedMetrics = new ArrayList<Metric>();

    public JSONMetricsContainer(String tenantId, List<JSONMetric> validJsonMetrics, List<ErrorResponse.ErrorData> validationErrors) {
        this.tenantId = tenantId;
        this.validMetrics = processJson(validJsonMetrics);
        this.validationErrors = validationErrors;
    }

    public List<Metric> getValidMetrics() {
        return validMetrics;
    }

    public List<ErrorResponse.ErrorData> getValidationErrors() {
        return validationErrors;
    }

    private List<Metric> processJson(List<JSONMetric> jsonMetrics) {

        List<Metric> metrics = new ArrayList<Metric>();

        for (JSONMetric jsonMetric : jsonMetrics) {

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