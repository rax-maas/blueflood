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
import java.util.List;
import java.util.Map;

public class BasicRollup extends Rollup {
    private static final Logger log = LoggerFactory.getLogger(BasicRollup.class);
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

    public void setCount(long count) {
        this.count = count;
    }
    
    public String toString() {
        return String.format("cnt:%d, avg:%s, var:%s, min:%s, max:%s", count, average, variance, minValue, maxValue);
    }

    @Override
    public void compute(Points input) throws IOException {
        if (input == null) {
            throw new IOException("Null input to create rollup from");
        }

        if (input.isEmpty()) {
            return;
        }

        Map<Long, Points.Point<Object>> points = input.getPoints();

        for (Map.Entry<Long, Points.Point<Object>> item : points.entrySet()) {
            Object value = item.getValue().getData();
            if (value instanceof Rollup) {
                BasicRollup basicRollup = (BasicRollup) value;
                this.count += basicRollup.getCount();
                average.handleRollupMetric(basicRollup);
                variance.handleRollupMetric(basicRollup);
                minValue.handleRollupMetric(basicRollup);
                maxValue.handleRollupMetric(basicRollup);
            } else {
                this.count += 1;
                average.handleFullResMetric(value);
                variance.handleFullResMetric(value);
                minValue.handleFullResMetric(value);
                maxValue.handleFullResMetric(value);
            }
        }
    }

    public static BasicRollup buildRollupFromInputData(Points input) throws IOException {
        final BasicRollup basicRollup = new BasicRollup();
        basicRollup.compute(input);

        return basicRollup;
    }
}