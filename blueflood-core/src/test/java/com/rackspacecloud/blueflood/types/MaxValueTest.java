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

import com.rackspacecloud.blueflood.rollup.Granularity;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MaxValueTest {
    private MaxValue max;

    @Before
    public void setUp() {
        max = new MaxValue();
    }

    @Test
    public void testMaxValueForDoubleMetrics() throws IOException {
        for (double val : TestData.DOUBLE_SRC) {
            max.handleFullResMetric(val);
        }
        Assert.assertTrue(max.isFloatingPoint());
        Assert.assertEquals(90.48232472545334, max.toDouble(), 0);
    }

    @Test
    public void testMaxValueForLongMetrics() throws IOException {
        for (long val : TestData.LONG_SRC) {
            max.handleFullResMetric(val);
        }
        Assert.assertTrue(!max.isFloatingPoint());
        Assert.assertEquals(94730802834L, max.toLong());
    }

    @Test
    public void testMaxValueWithMixedTypes() throws IOException {
        max.handleFullResMetric(6L);    // long
        max.handleFullResMetric(6.0);   // double
        max.handleFullResMetric(1);     // integer
        max.handleFullResMetric(99.0);  // double

        // The max value in the input set is 99.0 which is of type double
        Assert.assertTrue(max.isFloatingPoint());
        // Assert that indeed 99.0 is the maximum value
        Assert.assertEquals(99.0, max.toDouble(), 0);
    }

    @Test
    public void testRollupMax() throws IOException {
        BasicRollup basicRollup1 = new BasicRollup();
        BasicRollup basicRollup2 = new BasicRollup();
        BasicRollup basicRollup3 = new BasicRollup();
        BasicRollup basicRollup4 = new BasicRollup();

        BasicRollup netBasicRollup;

        Points input = Points.create(Granularity.FULL);
        input.add(new Points.Point<Object>(123456789L, 5L));
        input.add(new Points.Point<Object>(123456790L, 1L));
        input.add(new Points.Point<Object>(123456791L, 7L));
        basicRollup1.compute(input);

        input = Points.create(Granularity.FULL);
        input.add(new Points.Point<Object>(123456789L, 9L));
        input.add(new Points.Point<Object>(123456790L, 0L));
        input.add(new Points.Point<Object>(123456791L, 1L));
        basicRollup2.compute(input);

        input = Points.create(Granularity.FULL);
        input.add(new Points.Point<Object>(123456789L, 2.14d));
        input.add(new Points.Point<Object>(123456790L, 1.14d));
        basicRollup3.compute(input);

        input = Points.create(Granularity.FULL);
        input.add(new Points.Point<Object>(123456789L, 3.14d));
        input.add(new Points.Point<Object>(123456790L, 5.67d));
        basicRollup4.compute(input);

        // handle homegenous metric types and see if we get the right max

        // type long
        netBasicRollup = new BasicRollup();
        input = Points.create(Granularity.MIN_20);
        input.add(new Points.Point<BasicRollup>(123456789L, basicRollup1));
        input.add(new Points.Point<BasicRollup>(123456790L, basicRollup2));
        netBasicRollup.compute(input);

        MaxValue max = netBasicRollup.getMaxValue();
        Assert.assertTrue(!max.isFloatingPoint());
        Assert.assertEquals(9L, max.toLong());

        // type double
        netBasicRollup = new BasicRollup();
        input = Points.create(Granularity.MIN_20);
        input.add(new Points.Point<BasicRollup>(123456789L, basicRollup3));
        input.add(new Points.Point<BasicRollup>(123456790L, basicRollup4));
        netBasicRollup.compute(input);

        max = netBasicRollup.getMaxValue();
        Assert.assertTrue(max.isFloatingPoint());
        Assert.assertEquals(5.67d, max.toDouble(), 0);

        // handle heterogenous metric types and see if we get the right max
        netBasicRollup = new BasicRollup();
        input = Points.create(Granularity.MIN_20);
        input.add(new Points.Point<BasicRollup>(123456789L, basicRollup2));
        input.add(new Points.Point<BasicRollup>(123456790L, basicRollup3));
        netBasicRollup.compute(input);

        max = netBasicRollup.getMaxValue();
        Assert.assertTrue(!max.isFloatingPoint());
        Assert.assertEquals(9L, max.toLong());
    }
}