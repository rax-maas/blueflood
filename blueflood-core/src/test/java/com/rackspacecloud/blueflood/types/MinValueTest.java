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
        Rollup rollup1 = new Rollup();
        Rollup rollup2 = new Rollup();
        Rollup rollup3 = new Rollup();
        Rollup rollup4 = new Rollup();

        Rollup netRollup = new Rollup();

        rollup1.handleFullResMetric(5L);
        rollup1.handleFullResMetric(1L);
        rollup1.handleFullResMetric(7L);

        rollup2.handleFullResMetric(9L);
        rollup2.handleFullResMetric(0L);
        rollup2.handleFullResMetric(1L);

        rollup3.handleFullResMetric(2.14d);
        rollup3.handleFullResMetric(1.14d);

        rollup4.handleFullResMetric(3.14d);
        rollup4.handleFullResMetric(5.67d);

        // handle homegenous metric types and see if we get the right min

        // type long
        netRollup.handleRollupMetric(rollup1);
        netRollup.handleRollupMetric(rollup2);

        MinValue min = netRollup.getMinValue();
        Assert.assertTrue(!min.isFloatingPoint());
        Assert.assertEquals(0L, min.toLong());

        // type double
        netRollup = new Rollup();
        netRollup.handleRollupMetric(rollup3);
        netRollup.handleRollupMetric(rollup4);

        min = netRollup.getMinValue();
        Assert.assertTrue(min.isFloatingPoint());
        Assert.assertEquals(1.14d, min.toDouble(), 0);

        // handle heterogenous metric types and see if we get the right min
        netRollup = new Rollup();
        netRollup.handleRollupMetric(rollup2);
        netRollup.handleRollupMetric(rollup3);

        min = netRollup.getMinValue();
        Assert.assertTrue(!min.isFloatingPoint());
        Assert.assertEquals(0L, min.toLong());
    }
}