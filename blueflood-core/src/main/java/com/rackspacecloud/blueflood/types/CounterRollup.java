package com.rackspacecloud.blueflood.types;

import java.io.IOException;

public class CounterRollup implements Rollup {
    
    private long numSamples = 0;
    private long count = 0;
    
    private CounterRollup(long count, long numSamples) {
        this.count = count;
        this.numSamples = numSamples;
    }
    
    public long getCount() {
        return count;
    }
    
    public long getNumSamples() { return numSamples; }
    
    public static CounterRollup buildRollupFromRawSamples(Points<SimpleNumber> input) throws IOException {
        long count = 0;
        long numSamples = 0;
        for (Points.Point<SimpleNumber> point : input.getPoints().values()) {
            count += (Long)point.getData().getValue();
            numSamples += 1;
        }
        return new CounterRollup(count, numSamples);
    }
    
    public static CounterRollup buildRollupFromCounterRollups(Points<CounterRollup> input) throws IOException {
        long count = 0;
        long numSamples = 0;
        for (Points.Point<CounterRollup> point : input.getPoints().values()) {
            count += point.getData().getCount();
            numSamples += point.getData().getNumSamples();
        }
        return new CounterRollup(count, numSamples);
    }
}
