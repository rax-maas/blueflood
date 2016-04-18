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
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class JSONMetricsContainer {
    private final String tenantId;
    private final List<JSONMetric> jsonMetrics;
    private List<Metric> delayedMetrics;
    private static final long delayedMetricsMillis = Configuration.getInstance().getLongProperty(CoreConfig.DELAYED_METRICS_MILLIS);
    private static final long pastDiff = Configuration.getInstance().getLongProperty( CoreConfig.BEFORE_CURRENT_COLLECTIONTIME_MS );
    private static final long futureDiff = Configuration.getInstance().getLongProperty( CoreConfig.AFTER_CURRENT_COLLECTIONTIME_MS );

    public JSONMetricsContainer(String tenantId, List<JSONMetric> metrics) {
        this.tenantId = tenantId;
        this.jsonMetrics = metrics;
    }

    public List<String> getValidationErrors() {

        List<String> errors = new ArrayList<String>();

        // Validate that any ScopedJSONMetric is actually scoped to a tenant.
        for (JSONMetric jsonMetric : this.jsonMetrics) {
            errors.addAll( jsonMetric.getValidationErrors() );
        }

        return errors;
    }

    public List<Metric> toMetrics() {
        if (jsonMetrics == null || jsonMetrics.isEmpty()) {
            return null;
        }

        final List<Metric> metrics = new ArrayList<Metric>();
        delayedMetrics = new ArrayList<Metric>();
        for (JSONMetric jsonMetric : jsonMetrics) {
            Locator locator;
            if (jsonMetric instanceof ScopedJSONMetric) {
                ScopedJSONMetric scopedMetric = (ScopedJSONMetric)jsonMetric;
                locator = Locator.createLocatorFromPathComponents(scopedMetric.getTenantId(), jsonMetric.getMetricName());
            } else {
                locator = Locator.createLocatorFromPathComponents(tenantId, jsonMetric.getMetricName());
            }

            if (jsonMetric.getMetricValue() != null) {
                final Metric metric = new Metric(locator, jsonMetric.getMetricValue(), jsonMetric.getCollectionTime(),
                        new TimeValue(jsonMetric.getTtlInSeconds(), TimeUnit.SECONDS), jsonMetric.getUnit());
                long nowMillis = new DateTime().getMillis();
                if (nowMillis - metric.getCollectionTime() > delayedMetricsMillis) {
                    delayedMetrics.add(metric);
                    Instrumentation.markDelayedMetricsReceived();
                }
                metrics.add(metric);
            }
        }

        return metrics;
    }

    public boolean areDelayedMetricsPresent() {
        return delayedMetrics.size() > 0;
    }

    public List<Metric> getDelayedMetrics() {
        return delayedMetrics;
    }

    // Jackson compatible class. Jackson uses reflection to call these methods and so they have to match JSON keys.
    public static class JSONMetric {
        private String metricName;
        private Object metricValue;
        private long collectionTime;
        private int ttlInSeconds;
        private String unit;

        public String getMetricName() {
            return metricName;
        }

        public void setMetricName(String metricName) {
            this.metricName = metricName;
        }

        public String getUnit() {
            return unit;
        }

        public void setUnit(String unit) {
            this.unit = unit;
        }

        public Object getMetricValue() {
            return metricValue;
        }

        public void setMetricValue(Object metricValue) {
            this.metricValue = metricValue;
        }

        public long getCollectionTime() {
            return collectionTime;
        }

        public void setCollectionTime(long collectionTime) {
            this.collectionTime = collectionTime;
        }

        public int getTtlInSeconds() {
            return this.ttlInSeconds;
        }

        public void setTtlInSeconds(int ttlInSeconds) {
            this.ttlInSeconds = ttlInSeconds;
        }

        @JsonIgnore
        public List<String> getValidationErrors() {

            long current = System.currentTimeMillis();

            List<String> errors = new ArrayList<String>();

            // collectionTime is an optional parameter
            if ( collectionTime > current + futureDiff )
                errors.add( "'" + metricName + "': 'collectionTime' '" + collectionTime + "' is more than '" + futureDiff + "' milliseconds into the future." );

            if ( collectionTime < current - pastDiff )
                errors.add( "'" + metricName + "': 'collectionTime' '" + collectionTime + "' is more than '" + pastDiff + "' milliseconds into the past." );

            return errors;
        }
    }

    public static class ScopedJSONMetric extends JSONMetric {
        private String tenantId;

        public String getTenantId() { return tenantId; }

        public void setTenantId(String tenantId) { this.tenantId = tenantId; }

        @Override
        @JsonIgnore
        public List<String> getValidationErrors() {

            List<String> errors = super.getValidationErrors();

            if( StringUtils.isBlank( tenantId ) ) {
                errors.add( "'" + getMetricName() + "': No tenantId is provided for the metric." );
            }

            return errors;
        }
    }
}
