/*
 * Copyright 2016 Rackspace
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

/**
 * The sub-metrics used by {@link com.rackspacecloud.blueflood.types.BasicRollup} and
 * {@link com.rackspacecloud.blueflood.types.BluefloodGaugeRollup}.
 * Common sub-metrics are:
 * <ul>
 *     <li>average</li>
 *     <li>max</li>
 *     <li>min</li>
 *     <li>sum</li>
 * </ul>
 */
public abstract class BaseRollup implements Rollup {
    private static final Logger log = LoggerFactory.getLogger( BaseRollup.class );
    public static final int NUM_STATS = 4;

    private Average average;
    private Variance variance;
    private MinValue minValue;
    private MaxValue maxValue;
    private long count;


    public BaseRollup() {
        this.average = new Average();
        this.variance = new Variance();
        this.minValue = new MinValue();
        this.maxValue = new MaxValue();
        this.count = 0;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof BaseRollup)) {
            return false;
        }

        BaseRollup otherBaseRollup = (BaseRollup)other;

        return (this.count == otherBaseRollup.getCount())
                && average.equals(otherBaseRollup.getAverage())
                && variance.equals(otherBaseRollup.getVariance())
                && minValue.equals(otherBaseRollup.getMinValue())
                && maxValue.equals(otherBaseRollup.getMaxValue());
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

    // merge rollups into this rollup.
    protected void computeFromRollupsHelper(Points<? extends IBaseRollup> input) throws IOException {
        if (input == null) {
            throw new IOException("Null input to create rollup from");
        }

        if (input.isEmpty()) {
            return;
        }

        // See this and get mind blown:
        // http://stackoverflow.com/questions/18907262/bounded-wildcard-related-compiler-error
        Map<Long, ? extends Points.Point<? extends IBaseRollup>> points = input.getPoints();

        for (Map.Entry<Long, ? extends Points.Point<? extends IBaseRollup>> item : points.entrySet()) {
            IBaseRollup rollup = item.getValue().getData();
            if (!(rollup instanceof BaseRollup)) {
                throw new IOException("Cannot create BaseRollup from type " + rollup.getClass().getName());
            }
            IBaseRollup baseRollup = (IBaseRollup) rollup;
            this.count += baseRollup.getCount();
            average.handleRollupMetric(baseRollup);
            variance.handleRollupMetric(baseRollup);
            minValue.handleRollupMetric(baseRollup);
            maxValue.handleRollupMetric(baseRollup);
        }
    }

    @Override
    public Boolean hasData() {
        return getCount() > 0;
    }

    public abstract RollupType getRollupType();
}
