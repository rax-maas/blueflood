package com.rackspacecloud.blueflood.outputs.formats;

import com.rackspacecloud.blueflood.types.Points;

public class MetricData {
    private final Points data;
    private final String unit;

    public MetricData(Points points, String unit) {
        this.data = points;
        this.unit = unit;
    }

    public Points getData() {
        return data;
    }

    public String getUnit() {
        return unit;
    }
}
