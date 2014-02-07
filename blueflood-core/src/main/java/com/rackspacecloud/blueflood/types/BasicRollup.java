/*
 * Copyright 2013 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.types;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;


public class BasicRollup implements Rollup, IBasicRollup {
    private static final Logger log = LoggerFactory.getLogger(BasicRollup.class);
    public static final int NUM_STATS = 4;
    
    private Average average;
    private Variance variance;
    private MinValue minValue;
    private MaxValue maxValue;
    private long count;

    public BasicRollup() {
        this.average = new Average();
        this.variance = new Variance();
        this.minValue = new MinValue();
        this.maxValue = new MaxValue();
        this.count = 0;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof BasicRollup)) {
            return false;
        }

        BasicRollup otherBasicRollup = (BasicRollup)other;

        return (this.count == otherBasicRollup.getCount())
                && average.equals(otherBasicRollup.getAverage())
                && variance.equals(otherBasicRollup.getVariance())
                && minValue.equals(otherBasicRollup.getMinValue())
                && maxValue.equals(otherBasicRollup.getMaxValue());
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

    public String toString() {
        return String.format("cnt:%d, avg:%s, var:%s, min:%s, max:%s", count, average, variance, minValue, maxValue);
    }
    
    // setters
    // should I have made these chainable like TimerRollup?
    
    public void setCount(long count) {
        this.count = count;
    }
    
    public void setMin(MinValue min) {
        this.minValue = min;
    }
    
    public void setMin(Number min) {
        AbstractRollupStat.set(this.minValue, min);
    }
    
    public void setMax(MaxValue max) {
        this.maxValue = max;
    }
    
    public void setMax(Number max) {
        AbstractRollupStat.set(this.maxValue, max);
    }
    
    public void setVariance(Variance var) {
        this.variance = var;
    }
    
    public void setVariance(Number var) {
        AbstractRollupStat.set(this.variance, var);
    }

    public void setAverage(Average avg) {
        this.average = avg;
    }
    
    public void setAverage(Number avg) {
        AbstractRollupStat.set(this.average, avg);
    }
    
    // merge simple numbers with this rollup.
    protected void computeFromSimpleMetrics(Points<SimpleNumber> input) throws IOException {
        if (input == null) {
            throw new IOException("Null input to create rollup from");
        }

        if (input.isEmpty()) {
            return;
        }

        Map<Long, Points.Point<SimpleNumber>> points = input.getPoints();
        for (Map.Entry<Long, Points.Point<SimpleNumber>> item : points.entrySet()) {
            this.count += 1;
            SimpleNumber numericMetric = item.getValue().getData();
            average.handleFullResMetric(numericMetric.getValue());
            variance.handleFullResMetric(numericMetric.getValue());
            minValue.handleFullResMetric(numericMetric.getValue());
            maxValue.handleFullResMetric(numericMetric.getValue());
        }
    }
    
    // allows incrementally updating this rollup. This isn't part of the public API, so is declared unsafe.
    public void computeFromSimpleMetricsUnsafe(Points<SimpleNumber> input) throws IOException {
        computeFromSimpleMetrics(input);
    }

    // merge rollups into this rollup.
    protected void computeFromRollups(Points<IBasicRollup> input) throws IOException {
        if (input == null) {
            throw new IOException("Null input to create rollup from");
        }

        if (input.isEmpty()) {
            return;
        }

        // See this and get mind blown:
        // http://stackoverflow.com/questions/18907262/bounded-wildcard-related-compiler-error
        Map<Long, ? extends Points.Point<? extends IBasicRollup>> points = input.getPoints();

        for (Map.Entry<Long, ? extends Points.Point<? extends IBasicRollup>> item : points.entrySet()) {
            IBasicRollup rollup = item.getValue().getData();
            if (!(rollup instanceof BasicRollup)) {
                throw new IOException("Cannot create BasicRollup from type " + rollup.getClass().getName());
            }
            BasicRollup basicRollup = (BasicRollup) rollup;
            this.count += basicRollup.getCount();
            average.handleRollupMetric(basicRollup);
            variance.handleRollupMetric(basicRollup);
            minValue.handleRollupMetric(basicRollup);
            maxValue.handleRollupMetric(basicRollup);
        }
    }
    
    // allows merging with this rollup with another rollup. This is declared unsafe because it isn't part of the 
    // rollup API.
    public void computeFromRollupsUnsafe(Points<IBasicRollup> input) throws IOException {
        computeFromRollups(input);
    }

    public static BasicRollup buildRollupFromRawSamples(Points<SimpleNumber> input) throws IOException {
        final BasicRollup basicRollup = new BasicRollup();
        basicRollup.computeFromSimpleMetrics(input);

        return basicRollup;
    }

    public static BasicRollup buildRollupFromRollups(Points<BasicRollup> input) throws IOException {
        final BasicRollup basicRollup = new BasicRollup();
        basicRollup.computeFromRollups(recast(input, IBasicRollup.class));
        return basicRollup;
    }
    
    // yay generics?
    public static <T extends IBasicRollup> Points<T> recast(Points<? extends BasicRollup> points, Class<T> type) {
        Points<T> newPoints = new Points<T>();
        for (Map.Entry<Long, ? extends Points.Point<? extends BasicRollup>> entry : points.getPoints().entrySet())
            newPoints.add(new Points.Point<T>(entry.getKey(), (T)entry.getValue().getData()));
        return newPoints;
    }

    @Override
    public Boolean hasData() {
        return getCount() > 0;
    }

    @Override
    public RollupType getRollupType() {
        return RollupType.BF_BASIC;
    }
}