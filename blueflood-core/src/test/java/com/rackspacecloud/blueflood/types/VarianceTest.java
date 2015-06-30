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

import org.apache.commons.lang.ArrayUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class VarianceTest {
    private Variance variance = null;
    private static final double ERROR_TOLERANCE = 0.01;   // in %

    public static double[] DOUBLE_SRC_REALLY_HIGH = new double[]{Long.MAX_VALUE, Long.MAX_VALUE - 1, Long.MAX_VALUE};

    public static double[] ZEROS = new double[] {0, 0, 0, 0};

    @Before
    public void setUp() {
        variance = new Variance();
    }

    @Test
    public void testFullResMetricVariance() {

        // Our implementation for variance (welford one pass)
        for (double val : TestData.DOUBLE_SRC) {
            variance.handleFullResMetric(val);
        }

        Results result = new Results();
        result.expectedVariance = computeRawVariance(TestData.DOUBLE_SRC);
        result.computedVariance = variance.toDouble();


        double delta = (result.computedVariance - result.expectedVariance);

        double errorPercent = 0.0;
        if (delta != 0) {
            errorPercent = delta/result.expectedVariance * 100;
        }
        Assert.assertTrue(Math.abs(errorPercent) < ERROR_TOLERANCE);
    }

    @Test
    public void testFullResMetricVarianceForZeros() {
        // Our implementation for variance (welford one pass)
        for (double val : ZEROS) {
            variance.handleFullResMetric(val);
        }

        Results result = new Results();
        result.expectedVariance = computeRawVariance(ZEROS);
        result.computedVariance = variance.toDouble();


        double delta = (result.computedVariance - result.expectedVariance);

        double errorPercent = 0.0;
        if (delta != 0) {
            errorPercent = delta/result.expectedVariance * 100;
        }
        Assert.assertTrue(Math.abs(errorPercent) < ERROR_TOLERANCE);
    }

    @Test
    public void testFullResMetricVarianceForOneSample() {
        variance.handleFullResMetric(3.14);
        Assert.assertEquals(0.0, variance.toDouble(), 0);
    }

    @Test
    public void testFullResMetricVarianceNumericalStability() {
        // Our implementation for variance (welford one pass)
        for (double val : DOUBLE_SRC_REALLY_HIGH) {
            variance.handleFullResMetric(val);
        }

        Results result = new Results();
        result.expectedVariance = computeRawVariance(DOUBLE_SRC_REALLY_HIGH);
        result.computedVariance = variance.toDouble();


        double delta = (result.computedVariance - result.expectedVariance);

        double errorPercent = 0.0;
        if (delta != 0) {
            errorPercent = delta/result.expectedVariance * 100;
        }
        Assert.assertTrue(Math.abs(errorPercent) < ERROR_TOLERANCE);
    }

    @Test
    public void testRollupVariance() throws IOException {
        int size = TestData.DOUBLE_SRC.length;

        int GROUPS = 4;

        // split the input samples into 4 groups
        int windowSize = size/GROUPS;
        double[][] input = new double[GROUPS][windowSize]; // 4 groups of 31 samples each

        int count = 0; int i = 0; int j = 0;
        for (double val : TestData.DOUBLE_SRC) {
            input[i][j] = val;
            j++; count++;

            if (count % windowSize == 0) {
                i++;
                j = 0;
            }
        }

        // Compute variance for the 4 groups [simulate 5 MIN rollups from raw points]
        List<BasicRollup> basicRollups = new ArrayList<BasicRollup>();
        List<Results> resultsList = new ArrayList<Results>();
        for (i = 0; i < GROUPS; i++) {
            Results r = new Results();

            Points<SimpleNumber> inputSlice = new Points<SimpleNumber>();
            int timeOffset = 0;
            for (double val : input[i]) {
                inputSlice.add(new Points.Point<SimpleNumber>(123456789L + timeOffset++, new SimpleNumber(val)));
            }

            BasicRollup basicRollup = BasicRollup.buildRollupFromRawSamples(inputSlice);

            r.expectedVariance = computeRawVariance(input[i]);
            r.computedVariance = basicRollup.getVariance().toDouble();
            r.expectedAverage = computeRawAverage(input[i]);
            r.computedAverage = basicRollup.getAverage().toDouble();
            basicRollups.add(basicRollup);
            resultsList.add(r);
        }

        // First check if individual rollup variances and averages are close to raw variance & average for the window
        // of samples
        for (i = 0; i < GROUPS; i++) {
            Results result = resultsList.get(i);

            assertWithinErrorPercent(result.computedAverage, result.expectedAverage);
            assertWithinErrorPercent(result.computedVariance, result.expectedVariance);
        }

        // Now compute net variance using rollup versions [simulate 10 min rollups by aggregating two 5 min rollups]
        Points<BasicRollup> inputData = new Points<BasicRollup>();
        inputData.add(new Points.Point<BasicRollup>(123456789L, basicRollups.get(0)));
        inputData.add(new Points.Point<BasicRollup>(123456790L, basicRollups.get(1)));
        BasicRollup basicRollup10min_0 = BasicRollup.buildRollupFromRollups(inputData);
        assertWithinErrorPercent(basicRollup10min_0.getAverage().toDouble(),
                computeRawAverage(ArrayUtils.addAll(input[0], input[1])));
        assertWithinErrorPercent(basicRollup10min_0.getVariance().toDouble(),
                computeRawVariance(ArrayUtils.addAll(input[0], input[1])));

        inputData = new Points<BasicRollup>();
        inputData.add(new Points.Point<BasicRollup>(123456789L, basicRollups.get(2)));
        inputData.add(new Points.Point<BasicRollup>(123456790L, basicRollups.get(3)));
        BasicRollup basicRollup10min_1 = BasicRollup.buildRollupFromRollups(inputData);
        assertWithinErrorPercent(basicRollup10min_1.getAverage().toDouble(),
                computeRawAverage(ArrayUtils.addAll(input[2], input[3])));
        assertWithinErrorPercent(basicRollup10min_1.getVariance().toDouble(),
                computeRawVariance(ArrayUtils.addAll(input[2], input[3])));

        // Simulate 20 min rollups by aggregating two 10 min rollups
        inputData = new Points<BasicRollup>();
        inputData.add(new Points.Point<BasicRollup>(123456789L, basicRollup10min_0));
        inputData.add(new Points.Point<BasicRollup>(123456790L, basicRollup10min_1));
        BasicRollup basicRollup20min_0 = BasicRollup.buildRollupFromRollups(inputData);

        assertWithinErrorPercent(basicRollup20min_0.getAverage().toDouble(),
                computeRawAverage(TestData.DOUBLE_SRC));
        assertWithinErrorPercent(basicRollup20min_0.getVariance().toDouble(),
                computeRawVariance(TestData.DOUBLE_SRC));
    }

    private double computeRawVariance(double[] input) {
        // calculate average
        double avg = computeRawAverage(input);

        // calculate variance
        double sum = 0;
        for (double val: input) {
            sum += Math.pow((val - avg), 2);
        }
        return  sum/input.length;
    }

    private double computeRawAverage(double[] input) {
        // calculate mean
        double avg = 0;
        for (double val : input) {
            avg += val;
        }
        avg /= input.length;

        return avg;
    }

    private class Results {
        public double expectedVariance;
        public double computedVariance;
        public double expectedAverage;
        public double computedAverage;
    }

    private void assertWithinErrorPercent(double computed, double expected) {
        double errorPercentVar = 0.0;
        double deltaVar = computed - expected;
        if (deltaVar != 0) {
            errorPercentVar = deltaVar/expected * 100;
        }
        Assert.assertTrue(Math.abs(errorPercentVar) < ERROR_TOLERANCE);
    }
}
