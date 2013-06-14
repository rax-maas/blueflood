package com.cloudkick.blueflood.inputs.formats;

import com.cloudkick.blueflood.types.Metric;

import java.util.List;

public abstract class MetricsContainer {
    public abstract List<Metric> toMetrics();
}
