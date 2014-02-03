package com.rackspacecloud.blueflood.types;

import com.rackspacecloud.blueflood.utils.Util;

import java.io.IOException;

public class CounterRollup implements Rollup {
    
    private Number count;
    private double rate; // per-second!

    /**
     * Number of pre-aggregated counters received by Blueflood
     */
    private int sampleCount;
    
    public CounterRollup() {
        this.rate = 0d;
        this.sampleCount = 0;
    }
    
    public CounterRollup withCount(Number count) {
        this.count = promoteToDoubleOrLong(count);
        return this;
    }
    
    public CounterRollup withRate(double rate) {
        this.rate = rate;
        return this;
    }

    public CounterRollup withSampleCount(int sampleCount) {
        this.sampleCount = sampleCount;
        return this;
    }
    
    public Number getCount() {
        return count;
    }
    
    public double getRate() {
        return rate;
    }

    public int getSampleCount() { return sampleCount; }
    
    private static Number promoteToDoubleOrLong(Number num) {
        if (num instanceof Float)
            return num.doubleValue();
        else if (num instanceof Integer)
            return num.longValue();
        return num;
    }

    @Override
    public String toString() {
        return String.format("count: %s, rate:%s", count.toString(), rate);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof CounterRollup))
            return false;
        
        CounterRollup other = (CounterRollup)obj;
        return this.getCount().equals(other.getCount())
                && this.rate == other.rate
                && this.getSampleCount() == other.getSampleCount();
    }
    
    public static CounterRollup buildRollupFromRawSamples(Points<SimpleNumber> input) throws IOException {
        long minTime = Long.MAX_VALUE;
        long maxTime = Long.MIN_VALUE;
        CounterRollup rollup = new CounterRollup();
        Number count = 0L;
        for (Points.Point<SimpleNumber> point : input.getPoints().values()) {
            count = sum(count, point.getData().getValue());
            minTime = Math.min(minTime, point.getTimestamp());
            maxTime = Math.max(maxTime, point.getTimestamp());
        }
        double numSeconds = (double)(maxTime - minTime);
        double rate = count.doubleValue() / numSeconds;
        return rollup.withCount(count).withRate(rate).withSampleCount(input.getPoints().size());
    }

    public static CounterRollup buildRollupFromCounterRollups(Points<CounterRollup> input) throws IOException {
        
        Number count = 0L;
        double seconds = 0;
        int sampleCount = 0;
        for (Points.Point<CounterRollup> point : input.getPoints().values()) {
            count = sum(count, point.getData().getCount());
            sampleCount = sampleCount + point.getData().getSampleCount();
            seconds += Util.safeDiv(point.getData().getCount().doubleValue(), point.getData().getRate());
        }
        double aggregateRate = Util.safeDiv(count.doubleValue(), seconds);

        return new CounterRollup().withCount(count).withRate(aggregateRate).withSampleCount(sampleCount);
    }
    
    private static Number sum(Number x, Number y) {
        boolean isDouble = x instanceof Double || x instanceof Float || y instanceof Double || y instanceof Float;
        if (isDouble)
            return x.doubleValue() + y.doubleValue();
        else
            return x.longValue() + y.longValue();
    }

    @Override
    public Boolean hasData() {
        return sampleCount > 0;
    }

    @Override
    public RollupType getRollupType() {
        return RollupType.COUNTER;
    }
}
