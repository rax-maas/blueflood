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


public class BasicRollup extends BaseRollup implements IBaseRollup {
    private static final Logger log = LoggerFactory.getLogger(BasicRollup.class);

    private double sum;

    public BasicRollup() {
        super();
        this.sum = 0;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof BasicRollup)) {
            return false;
        }

        BasicRollup otherBasicRollup = (BasicRollup)other;

        return super.equals( otherBasicRollup )
                && sum == otherBasicRollup.getSum();
    }

    public double getSum() {
        return sum;
    }

    public String toString() {
        return String.format("cnt:%d, avg:%s, var:%s, min:%s, max:%s, sum: %s", getCount(), getAverage(), getVariance(),
                getMinValue(), getMaxValue(), sum);
    }

    public void setSum(double s) {

        sum = s;
    }

    // merge simple numbers with this rollup.
    protected void computeFromSimpleMetrics(Points<SimpleNumber> input) throws IOException {

        super.computeFromSimpleMetrics( input );

        if (input.isEmpty()) {
            return;
        }

        Map<Long, Points.Point<SimpleNumber>> points = input.getPoints();
        for (Map.Entry<Long, Points.Point<SimpleNumber>> item : points.entrySet()) {
            SimpleNumber numericMetric = item.getValue().getData();
            sum += numericMetric.getValue().doubleValue();
        }
    }

    // merge rollups into this rollup.
    protected void computeFromRollups(Points<BasicRollup> input) throws IOException {

        computeFromRollupsHelper( input );

        if (input.isEmpty()) {
            return;
        }

        // See this and get mind blown:
        // http://stackoverflow.com/questions/18907262/bounded-wildcard-related-compiler-error
        Map<Long, ? extends Points.Point<? extends BasicRollup>> points = input.getPoints();

        for (Map.Entry<Long, ? extends Points.Point<? extends BasicRollup>> item : points.entrySet()) {
            BasicRollup rollup = item.getValue().getData();
            if (!(rollup instanceof BasicRollup)) {
                throw new IOException("Cannot create BasicRollup from type " + rollup.getClass().getName());
            }
            BasicRollup basicRollup = (BasicRollup) rollup;
            sum += basicRollup.getSum();
        }
    }

    public static BasicRollup buildRollupFromRawSamples(Points<SimpleNumber> input) throws IOException {
        final BasicRollup basicRollup = new BasicRollup();
        basicRollup.computeFromSimpleMetrics(input);

        return basicRollup;
    }

    public static BasicRollup buildRollupFromRollups(Points<BasicRollup> input) throws IOException {
        final BasicRollup basicRollup = new BasicRollup();
        basicRollup.computeFromRollups(input);
        return basicRollup;
    }

    @Override
    public RollupType getRollupType() {
        return RollupType.BF_BASIC;
    }
}