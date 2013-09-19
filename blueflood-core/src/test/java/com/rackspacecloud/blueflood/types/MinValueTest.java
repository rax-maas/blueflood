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

public class MinValueTest {
    private MinValue min;

    @Before
    public void setUp() {
        min = new MinValue();
    }

    @Test
    public void testMinValueForDoubleMetrics() throws IOException {
        for (double val : TestData.DOUBLE_SRC) {
            min.handleFullResMetric(val);
        }
        Assert.assertTrue(min.isFloatingPoint());
        Assert.assertEquals(0.0, min.toDouble(), 0);
    }

    @Test
    public void testMinValueForLongMetrics() throws IOException {
        for (long val : TestData.LONG_SRC) {
            min.handleFullResMetric(val);
        }
        Assert.assertTrue(!min.isFloatingPoint());
        Assert.assertEquals(12L, min.toLong());
    }

    @Test
    public void testMinValueWithMixedTypes() throws IOException {
        min.handleFullResMetric(6L);    // long
        min.handleFullResMetric(6.0);   // double
        min.handleFullResMetric(1);     // integer
        min.handleFullResMetric(99.0);  // double

        // The minimum value in the input set is 1 which is of type Long
        Assert.assertTrue(!min.isFloatingPoint());
        // Assert that indeed 1 is the minimum value
        Assert.assertEquals(1, min.toLong());
    }

    @Test
    public void testRollupMin() throws IOException {
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

        input = Points.create(Granularity.MIN_5);
        BasicRollup temp = new BasicRollup();
        temp.getMinValue().setDoubleValue(2.14);
        input.add(new Points.Point<BasicRollup>(123456789L, temp));
        temp.getMinValue().setDoubleValue(1.14);
        input.add(new Points.Point<BasicRollup>(123456790L, temp));
        basicRollup3.compute(input);

        input = Points.create(Granularity.MIN_5);
        temp = new BasicRollup();
        temp.getMinValue().setDoubleValue(3.14);
        input.add(new Points.Point<BasicRollup>(123456789L, temp));
        temp.getMinValue().setDoubleValue(5.67);
        input.add(new Points.Point<BasicRollup>(123456790L, temp));
        basicRollup4.compute(input);

        // handle homegenous metric types and see if we get the right min

        // type long
        netBasicRollup = new BasicRollup();
        input = Points.create(Granularity.MIN_20);
        input.add(new Points.Point<BasicRollup>(123456789L, basicRollup1));
        input.add(new Points.Point<BasicRollup>(123456790L, basicRollup2));
        netBasicRollup.compute(input);

        MinValue min = netBasicRollup.getMinValue();
        Assert.assertTrue(!min.isFloatingPoint());
        Assert.assertEquals(0L, min.toLong());

        // type double
        netBasicRollup = new BasicRollup();
        input = Points.create(Granularity.MIN_20);
        input.add(new Points.Point<BasicRollup>(123456789L, basicRollup3));
        input.add(new Points.Point<BasicRollup>(123456790L, basicRollup4));
        netBasicRollup.compute(input);

        min = netBasicRollup.getMinValue();
        Assert.assertTrue(min.isFloatingPoint());
        Assert.assertEquals(1.14d, min.toDouble(), 0);

        // handle heterogenous metric types and see if we get the right min
        netBasicRollup = new BasicRollup();
        input = Points.create(Granularity.MIN_20);
        input.add(new Points.Point<BasicRollup>(123456789L, basicRollup2));
        input.add(new Points.Point<BasicRollup>(123456790L, basicRollup3));
        netBasicRollup.compute(input);

        min = netBasicRollup.getMinValue();
        Assert.assertTrue(!min.isFloatingPoint());
        Assert.assertEquals(0L, min.toLong());
    }
}