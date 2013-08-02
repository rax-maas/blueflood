package com.cloudkick.blueflood.types;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

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

        // handle homegenous metric types and see if we get the right max

        // type long
        netRollup = new Rollup();
        netRollup.handleRollupMetric(rollup1);
        netRollup.handleRollupMetric(rollup2);

        MaxValue max = netRollup.getMaxValue();
        Assert.assertTrue(!max.isFloatingPoint());
        Assert.assertEquals(9L, max.toLong());

        // type double
        netRollup = new Rollup();
        netRollup.handleRollupMetric(rollup3);
        netRollup.handleRollupMetric(rollup4);

        max = netRollup.getMaxValue();
        Assert.assertTrue(max.isFloatingPoint());
        Assert.assertEquals(5.67d, max.toDouble(), 0);

        // handle heterogenous metric types and see if we get the right max
        netRollup = new Rollup();
        netRollup.handleRollupMetric(rollup2);
        netRollup.handleRollupMetric(rollup3);

        max = (MaxValue) netRollup.getMaxValue();
        Assert.assertTrue(!max.isFloatingPoint());
        Assert.assertEquals(9L, max.toLong());
    }
}