package com.cloudkick.blueflood.types;

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
        List<Rollup> rollups = new ArrayList<Rollup>();
        List<Results> resultsList = new ArrayList<Results>();
        for (i = 0; i < GROUPS; i++) {
            Rollup rollup = new Rollup();
            Results r = new Results();

            for (double val : input[i]) {
                rollup.handleFullResMetric(val);
            }

            r.expectedVariance = computeRawVariance(input[i]);
            r.computedVariance = rollup.getVariance().toDouble();
            r.expectedAverage = computeRawAverage(input[i]);
            r.computedAverage = rollup.getAverage().toDouble();
            rollups.add(rollup);
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
        Rollup rollup10min_0 = new Rollup();
        rollup10min_0.handleRollupMetric(rollups.get(0));
        rollup10min_0.handleRollupMetric(rollups.get(1));
        assertWithinErrorPercent(rollup10min_0.getAverage().toDouble(),
                computeRawAverage(ArrayUtils.addAll(input[0], input[1])));
        assertWithinErrorPercent(rollup10min_0.getVariance().toDouble(),
                computeRawVariance(ArrayUtils.addAll(input[0], input[1])));

        Rollup rollup10min_1 = new Rollup();
        rollup10min_1.handleRollupMetric(rollups.get(2));
        rollup10min_1.handleRollupMetric(rollups.get(3));
        assertWithinErrorPercent(rollup10min_1.getAverage().toDouble(),
                computeRawAverage(ArrayUtils.addAll(input[2], input[3])));
        assertWithinErrorPercent(rollup10min_1.getVariance().toDouble(),
                computeRawVariance(ArrayUtils.addAll(input[2], input[3])));

        // Simulate 20 min rollups by aggregating two 10 min rollups
        Rollup rollup20min_0 = new Rollup();
        rollup20min_0.handleRollupMetric(rollup10min_0);
        rollup20min_0.handleRollupMetric(rollup10min_1);

        assertWithinErrorPercent(rollup20min_0.getAverage().toDouble(),
                computeRawAverage(TestData.DOUBLE_SRC));
        assertWithinErrorPercent(rollup20min_0.getVariance().toDouble(),
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
