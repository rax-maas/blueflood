package com.rackspacecloud.blueflood.types;

import java.io.IOException;

public class CounterRollup extends SingleValueRollup {
    
    public CounterRollup(int numSamples) {
        this.numSamples = numSamples;
    }
    
    public CounterRollup withCount(long count) {
        return (CounterRollup)this.withValue(count);
    }
    
    public long getCount() {
        return getValue().longValue();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CounterRollup))
            return false;
        return this.getValue().equals(((CounterRollup)obj).getValue());
    }

    public static CounterRollup buildRollupFromCounterRollups(Points<CounterRollup> input) throws IOException {
        long count = 0;
        int numSamples = 0;
        for (Points.Point<CounterRollup> point : input.getPoints().values()) {
            count += point.getData().getCount();
            numSamples += point.getData().numSamples;
        }
        return new CounterRollup(numSamples).withCount(count);
    }
}
