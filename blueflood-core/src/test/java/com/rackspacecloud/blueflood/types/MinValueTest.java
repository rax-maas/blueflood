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

        BasicRollup netBasicRollup = new BasicRollup();

        List<Object> input = new ArrayList<Object>();
        input.add(5L); input.add(1L); input.add(7L);
        basicRollup1.compute(input);

        input = new ArrayList<Object>();
        input.add(9L); input.add(0L); input.add(1L);
        basicRollup2.compute(input);

        input = new ArrayList<Object>();
        input.add(2.14d); input.add(1.14d);
        basicRollup3.compute(input);

        input = new ArrayList<Object>();
        input.add(3.14d); input.add(5.67d);
        basicRollup4.compute(input);

        // handle homegenous metric types and see if we get the right min

        // type long
        input = new ArrayList<Object>();
        input.add(basicRollup1); input.add(basicRollup2);
        netBasicRollup.compute(input);

        MinValue min = netBasicRollup.getMinValue();
        Assert.assertTrue(!min.isFloatingPoint());
        Assert.assertEquals(0L, min.toLong());

        // type double
        netBasicRollup = new BasicRollup();
        input = new ArrayList<Object>();
        input.add(basicRollup3); input.add(basicRollup4);
        netBasicRollup.compute(input);

        min = netBasicRollup.getMinValue();
        Assert.assertTrue(min.isFloatingPoint());
        Assert.assertEquals(1.14d, min.toDouble(), 0);

        // handle heterogenous metric types and see if we get the right min
        netBasicRollup = new BasicRollup();
        input = new ArrayList<Object>();
        input.add(basicRollup2); input.add(basicRollup3);

        min = netBasicRollup.getMinValue();
        Assert.assertTrue(!min.isFloatingPoint());
        Assert.assertEquals(0L, min.toLong());
    }
}