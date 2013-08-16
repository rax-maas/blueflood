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

import com.rackspacecloud.blueflood.io.Constants;

import java.util.ArrayList;
import java.util.List;

public class Variance extends AbstractRollupStat {
    private long count = 0;

    // These are required for welford algorithm which works on raw samples
    private double mean;
    private double M2;
    private double populationVariance; // variance we are actually interested in

    // These are requied for rollup variance calculation
    private List<Rollup> rollupList;

    private boolean isRollup;
    
    private boolean needsCompute = false;

    public Variance() {
        super();

        this.mean = 0;
        this.M2 = 0;
        this.populationVariance = 0;
        this.rollupList = new ArrayList<Rollup>();
        this.isRollup = false;
    }

    @Override
    public boolean isFloatingPoint() {
        return true;
    }

    @Override
    void handleFullResMetric(Object o) throws RuntimeException {
        // Welford algorithm (one pass)
        double input = getDoubleValue(o);
        double delta = input - mean;
        this.count++;
        this.mean = this.mean + (delta/this.count);
        this.M2 = this.M2 + delta * (input - mean);
        this.populationVariance = this.M2/(this.count);
        this.setDoubleValue(this.populationVariance);
    }

    @Override
    void handleRollupMetric(Rollup rollup) throws RuntimeException {
        this.needsCompute = true;
        this.isRollup = true;
        rollupList.add(rollup); // we need all the rollup metrics before computing the final variance.
    }

    private synchronized void compute() {
        if (!needsCompute)
            return;
        needsCompute = false;
        double grandMean = 0.0;
        long totalSampleSize = 0L;

        if (this.isRollup) {
            double sum1 = 0;
            double sum2 = 0;

            // first pass to compute grand mean over all windows
            for (Rollup rollup : rollupList) {
                Average avg = rollup.getAverage();
                totalSampleSize += rollup.getCount();

                double avgVal;
                if (!avg.isFloatingPoint()) {
                    avgVal = (double) avg.toLong();
                } else {
                    avgVal = avg.toDouble();
                }

                grandMean += rollup.getCount() * avgVal;
            }

            if (totalSampleSize != 0) {
                grandMean = grandMean/totalSampleSize;
            } else {
                this.setDoubleValue(0.0); // no samples found
                return;
            }

            // With grand mean known, compute overall variance using
            // standard textbook variance estimation over windows with varying sample sizes.
            // The formula is exact and its precision depends on variance over a single window
            // which is computed using Welford. Except for numerical instability problems, Welford
            // is almost exact.
            for (Rollup rollup : rollupList) {
                Variance var = rollup.getVariance();
                Average avg = rollup.getAverage();
                sum1 += rollup.getCount() * var.toDouble();

                double avgVal;
                if (!avg.isFloatingPoint()) {
                    avgVal = (double) avg.toLong();
                } else {
                    avgVal = avg.toDouble();
                }

                sum2 += rollup.getCount() * Math.pow((avgVal - grandMean), 2);
            }

            this.setDoubleValue((sum1 + sum2) / totalSampleSize);
        }
    }

    @Override
    public double toDouble() {
        if (needsCompute)
            compute();
        return super.toDouble();
    }

    private double getDoubleValue(Object number) {
        double val = 0;
        if (number instanceof Integer) {
            val = ((Integer) number).doubleValue();
        } else if (number instanceof Long) {
            val = ((Long) number).doubleValue();
        } else if (number instanceof Double) {
            val = (Double)number;
        }

        return val;
    }

    @Override
    public long toLong() {
        throw new IllegalStateException("No long value for variances");    
    }

    @Override
    public byte getStatType() {
        return Constants.VARIANCE;
    }
}