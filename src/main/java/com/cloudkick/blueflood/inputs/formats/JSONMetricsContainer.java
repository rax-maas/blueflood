package com.cloudkick.blueflood.inputs.formats;

import com.cloudkick.blueflood.types.Locator;
import com.cloudkick.blueflood.types.Metric;
import com.cloudkick.blueflood.utils.TimeValue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class JSONMetricsContainer extends MetricsContainer {
    private List<JSONMetric> jsonMetrics;

    public JSONMetricsContainer(List<JSONMetric> metrics) {
        this.jsonMetrics = metrics;
    }

    @Override
    public List<Metric> toMetrics() {
        if (jsonMetrics == null || jsonMetrics.isEmpty()) {
            return null;
        }

        final List<Metric> metrics = new ArrayList<Metric>();
        for (JSONMetric jsonMetric : jsonMetrics) {
            final Locator locator = Locator.createLocatorFromAccountIdAndName(jsonMetric.getAccountId(),
                    jsonMetric.getMetricName());
            final Metric metric = new Metric(locator, jsonMetric.getMetricValue(), jsonMetric.getCollectionTime(),
                    new TimeValue(jsonMetric.getTtlInSeconds(), TimeUnit.SECONDS), jsonMetric.getUnit());
            metrics.add(metric);
        }

        return metrics;
    }

    // Jackson compatible class. Jackson uses reflection to call these methods and so they have to match JSON keys.
    public static class JSONMetric {
        private String accountId;
        private String metricName;
        private Object metricValue;
        private long collectionTime;
        private int ttlInSeconds;
        private String unit;

        public String getAccountId() {
            return accountId;
        }

        public void setAccountId(String accountId) {
            this.accountId = accountId;
        }

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
            return ttlInSeconds;
        }

        public void setTtlInSeconds(int ttlInSeconds) {
            this.ttlInSeconds = ttlInSeconds;
        }
    }
}
