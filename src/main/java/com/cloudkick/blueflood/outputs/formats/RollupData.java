package com.cloudkick.blueflood.outputs.formats;

import com.cloudkick.blueflood.types.Points;

public class RollupData {
    private final Points data;
    private String unit;

    public RollupData(Points points, String unit) {
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
