package com.rackspacecloud.blueflood.io;

import java.util.*;

public class MetricNameListBuilder {

    /**
     * Given a metric name foo.bar.baz.qux, for query foo.bar.*
     *
     * metricNamesWithNextLevelSet will accumulate all metric names which have next level.
     *      metricNamesWithNextLevelSet  -> {foo.bar.baz, false}
     *
     * completeMetricNamesSet will accumulate all complete metric names matching query
     *      regular metric: foo.bar.test
     *      completeMetricNamesSet  -> {foo.bar.test, true}
     */

    private final Set<MetricName> metricNamesWithNextLevelSet = new LinkedHashSet<MetricName>();
    private final Set<MetricName> completeMetricNamesSet = new LinkedHashSet<MetricName>();


    public MetricNameListBuilder addMetricNameWithNextLevel(String metricName) {
        metricNamesWithNextLevelSet.add(new MetricName(metricName, false));
        return this;
    }

    public MetricNameListBuilder addMetricNameWithNextLevel(Set<String> metricNames) {
        for (String metricName: metricNames) {
            addMetricNameWithNextLevel(metricName);
        }
        return this;
    }

    public MetricNameListBuilder addCompleteMetricName(String metricName) {
        completeMetricNamesSet.add(new MetricName(metricName, true));
        return this;
    }

    public MetricNameListBuilder addCompleteMetricName(Set<String> metricNames) {
        for (String metricName: metricNames) {
            addCompleteMetricName(metricName);
        }
        return this;
    }

    public ArrayList<MetricName> build() {

        final ArrayList<MetricName> resultList = new ArrayList<MetricName>();
        resultList.addAll(metricNamesWithNextLevelSet);
        resultList.addAll(completeMetricNamesSet);

        return resultList;
    }
}