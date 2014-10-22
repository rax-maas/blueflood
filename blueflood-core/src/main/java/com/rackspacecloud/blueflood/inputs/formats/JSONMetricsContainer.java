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

import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class JSONMetricsContainer {
    private final String tenantId;
    private final List<JSONMetric> jsonMetrics;

    public JSONMetricsContainer(String tenantId, List<JSONMetric> metrics) {
        this.tenantId = tenantId;
        this.jsonMetrics = metrics;
    }

    public boolean isValid() {
        // Validate that any ScopedJSONMetric is actually scoped to a tenant.
        for (JSONMetric jsonMetric : this.jsonMetrics) {
            if (!jsonMetric.isValid()) {
                return false;
            }
        }
        return true;
    }

    public List<Metric> toMetrics() {
        if (jsonMetrics == null || jsonMetrics.isEmpty()) {
            return null;
        }

        final List<Metric> metrics = new ArrayList<Metric>();
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
                metrics.add(metric);
            }
        }

        return metrics;
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
        public boolean isValid() {
            return true;
        }
    }

    public static class ScopedJSONMetric extends JSONMetric {
        private String tenantId;

        public String getTenantId() { return tenantId; }

        public void setTenantId(String tenantId) { this.tenantId = tenantId; }

        @JsonIgnore
        public boolean isValid() {
            return (tenantId != null && super.isValid());
        }
    }
}
