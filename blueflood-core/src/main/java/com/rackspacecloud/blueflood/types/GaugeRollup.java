package com.rackspacecloud.blueflood.types;

import java.io.IOException;
import java.util.Map;

public class GaugeRollup extends BasicRollup {
    public static final Points.Point<SimpleNumber> NEVER_HAPPENED = new Points.Point(-1, null);
    private Points.Point<SimpleNumber> latestValue;

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof GaugeRollup))
            return false;
        else {
            GaugeRollup other = (GaugeRollup)obj;
            return super.equals(other) && other.latestValue.equals(this.latestValue);
        }
    }
    
    public long getTimestamp() {
        return latestValue.getTimestamp();
    }
    
    public Number getLatestNumericValue() {
        return latestValue.getData().getValue();
    }
    
    public SimpleNumber getLatestValue() {
        return latestValue.getData();
    }

    public static GaugeRollup buildFromRawSamples(Points<SimpleNumber> input) throws IOException {
        
        // normal stuff.
        GaugeRollup rollup = new GaugeRollup();
        rollup.computeFromSimpleMetrics(input);
        
        // latest value is special.
        Points.Point<SimpleNumber> latest = null;
        for (Map.Entry<Long, Points.Point<SimpleNumber>> entry : input.getPoints().entrySet()) {
            if (latest == null || entry.getValue().getTimestamp() > latest.getTimestamp())
                latest = entry.getValue();
        }
        
        if (latest == null)
            latest = NEVER_HAPPENED;
        rollup.latestValue = latest;
        
        return rollup;
    }
    
    public static GaugeRollup buildFromGaugeRollups(Points<GaugeRollup> input) throws IOException {
        GaugeRollup rollup = new GaugeRollup();
        
        rollup.computeFromRollups(BasicRollup.recast(input, IBasicRollup.class));
        
        Points.Point<SimpleNumber> latest = rollup.latestValue;
        
        for (Map.Entry<Long, Points.Point<GaugeRollup>> entry : input.getPoints().entrySet()) {
            if (latest == null || entry.getValue().getTimestamp() > latest.getTimestamp())
                latest = entry.getValue().getData().latestValue;
        }
        
        if (latest == null)
            latest = NEVER_HAPPENED;
        rollup.latestValue = latest;
        
        return rollup;
    }
    
    public static GaugeRollup fromBasicRollup(IBasicRollup basic, Points.Point<SimpleNumber> latestValue) {
        GaugeRollup rollup = new GaugeRollup();
        
        rollup.setCount(basic.getCount());
        rollup.setAverage((Average)basic.getAverage());
        rollup.setMin((MinValue)basic.getMinValue());
        rollup.setMax((MaxValue)basic.getMaxValue());
        rollup.setVariance((Variance)basic.getVariance());
        rollup.latestValue = latestValue;
        
        return rollup;
    }
}
