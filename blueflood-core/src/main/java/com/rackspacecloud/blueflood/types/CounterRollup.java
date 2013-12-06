package com.rackspacecloud.blueflood.types;

import java.io.IOException;

public class CounterRollup extends SingleValueRollup {
    
    public CounterRollup(int numSamples) {
        this.numSamples = numSamples;
    }
    
    public CounterRollup withCount(Number count) {
        return (CounterRollup)this.withValue(count);
    }
    
    public Number getCount() {
        return getValue();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CounterRollup))
            return false;
        return this.getValue().equals(((CounterRollup)obj).getValue());
    }
    
    public static CounterRollup buildRollupFromRawSamples(Points<SimpleNumber> input) throws IOException {
        CounterRollup rollup = new CounterRollup(input.getPoints().size());
        Number count = 0L;
        for (Points.Point<SimpleNumber> point : input.getPoints().values()) {
            count = sum(count, (Number)point.getData().getValue());
        }
        return rollup.withCount(count);
    }

    public static CounterRollup buildRollupFromCounterRollups(Points<CounterRollup> input) throws IOException {
        Number count = 0L;
        int numSamples = 0;
        for (Points.Point<CounterRollup> point : input.getPoints().values()) {
            count = sum(count, point.getData().getCount());
            numSamples += point.getData().numSamples;
        }
        return new CounterRollup(numSamples).withCount(count);
    }
    
    private static Number sum(Number x, Number y) {
        boolean isDouble = x instanceof Double || x instanceof Float || y instanceof Double || y instanceof Float;
        if (isDouble)
            return x.doubleValue() + y.doubleValue();
        else
            return x.longValue() + y.longValue();
    }
}
