package com.cloudkick.blueflood.types;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

public class MetricsCollection {
    private final List<Metric> metrics;

    public MetricsCollection() {
        this.metrics = new ArrayList<Metric>();
    }

    public void add(List<Metric> other) {
        metrics.addAll(other);
    }

    public List<Metric> getMetrics() {
        return metrics;
    }

    public int size() {
        return metrics.size();
    }

    public static List<List<Metric>> getMetricsAsBatches(MetricsCollection collection, int partitions) {
        if (partitions <= 0) {
            partitions = 1;
        }

        int sizePerBatch = collection.size()/partitions + 1;

        return Lists.partition(collection.getMetrics(), sizePerBatch);
    }
}
