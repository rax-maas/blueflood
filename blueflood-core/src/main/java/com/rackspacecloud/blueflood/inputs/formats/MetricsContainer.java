package com.rackspacecloud.blueflood.inputs.formats;

import com.rackspacecloud.blueflood.types.Metric;

import java.util.List;

public abstract class MetricsContainer {
    public abstract List<Metric> toMetrics();
}
