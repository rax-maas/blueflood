package com.cloudkick.blueflood.types;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Rollup {
    private static final Logger log = LoggerFactory.getLogger(Rollup.class);
    private Average average;
    private Variance variance;
    private MinValue minValue;
    private MaxValue maxValue;
    private long count;

    public Rollup() {
        this.average = new Average();
        this.variance = new Variance();
        this.minValue = new MinValue();
        this.maxValue = new MaxValue();
        this.count = 0;
    }

    public void handleFullResMetric(Object o) throws IOException {
        average.handleFullResMetric(o);
        variance.handleFullResMetric(o);
        minValue.handleFullResMetric(o);
        maxValue.handleFullResMetric(o);
        this.count++;
    }

    public void handleRollupMetric(Rollup rollup) throws IOException {
        average.handleRollupMetric(rollup);
        variance.handleRollupMetric(rollup);
        minValue.handleRollupMetric(rollup);
        maxValue.handleRollupMetric(rollup);
        this.count += rollup.getCount();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Rollup)) {
            return false;
        }

        Rollup otherRollup = (Rollup)other;

        return (this.count == otherRollup.getCount())
                && average.equals(otherRollup.getAverage())
                && variance.equals(otherRollup.getVariance())
                && minValue.equals(otherRollup.getMinValue())
                && maxValue.equals(otherRollup.getMaxValue());
    }

    public Average getAverage() {
        return this.average;
    }

    public Variance getVariance() {
        return this.variance;
    }

    public MinValue getMinValue() {
        return this.minValue;
    }

    public MaxValue getMaxValue() {
        return this.maxValue;
    }

    public long getCount() {
        return this.count;
    }

    public void setCount(long count) {
        this.count = count;
    }
    
    public String toString() {
        return String.format("cnt:%d, avg:%s, var:%s, min:%s, max:%s", count, average, variance, minValue,  maxValue);
    }
}