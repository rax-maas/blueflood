package com.rackspacecloud.blueflood.types;

import java.io.IOException;

public class GaugeRollup extends SingleValueRollup {
    
    public GaugeRollup withGauge(Number gauge) {
        return (GaugeRollup) this.withValue(gauge);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof GaugeRollup))
            return false;
        else
            return getValue().equals(((GaugeRollup)obj).getValue());
    }

    public static GaugeRollup buildFromGaugeRollups(Points<GaugeRollup> input) throws IOException {
        // return the one with the latest timestamp.
        int numSamples = 0;
        Points.Point<GaugeRollup> latest = null;
        for (Points.Point<GaugeRollup> point : input.getPoints().values()) {
            if (latest == null)
                latest = point;
            else if (latest.getTimestamp() < point.getTimestamp())
                latest = point;
            numSamples += point.getData().getNumSamplesUnsafe();
        }
        GaugeRollup newGauge = new GaugeRollup().withGauge(latest.getData().getValue());
        newGauge.numSamples = numSamples;
        return newGauge;
    }
}
