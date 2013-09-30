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

import com.bigml.histogram.*;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class HistogramRollup extends Rollup {
    private final Histogram<SimpleTarget> histogram;
    public static Integer MAX_BIN_SIZE = 64;

    private HistogramRollup(int bins) {
        if (bins > MAX_BIN_SIZE) {
            bins = MAX_BIN_SIZE;
        }
        this.histogram = new Histogram<SimpleTarget>(bins);
    }

    public HistogramRollup(Collection<Bin<SimpleTarget>> bins) {
        this.histogram = new Histogram(bins.size());
        for (Bin<SimpleTarget> bin : bins) {
            this.histogram.insertBin(bin);
        }
    }

    @Override
    public void computeFromRollups(Points<? extends Rollup> input) throws IOException {
        if (input == null) {
            throw new IOException("Null input to create rollup from");
        }

        if (input.isEmpty()) {
            return;
        }

        Map<Long, ? extends Points.Point<? extends Rollup>> points = input.getPoints();

        for (Map.Entry<Long, ? extends Points.Point<? extends Rollup>> item : points.entrySet()) {
            Rollup rollup = item.getValue().getData();
            if (!(rollup instanceof HistogramRollup)) {
                throw new IOException("Cannot create HistogramRollup from type " + rollup.getClass().getName());
            }
            HistogramRollup otherHistogram = (HistogramRollup) rollup;
            try {
                histogram.merge(otherHistogram.histogram);
            } catch (MixedInsertException ex) {
                throw new IOException(ex);
            }
        }
    }

    @Override
    public void computeFromSimpleMetrics(Points<SimpleNumber> input) throws IOException {
        try {
            for (Map.Entry<Long, Points.Point<SimpleNumber>> item : input.getPoints().entrySet()) {
                histogram.insert(toDouble(item.getValue().getData().getValue()));
            }
        } catch (MixedInsertException ex) {
            throw new IOException(ex);
        }
    }

    public static HistogramRollup buildRollupFromRawSamples(Points<SimpleNumber> input) throws IOException {
        int number_of_bins = getIdealNumberOfBins(input);
        final HistogramRollup histogramRollup = new HistogramRollup(number_of_bins);
        histogramRollup.computeFromSimpleMetrics(input);

        return histogramRollup;
    }

    public static HistogramRollup buildRollupFromRollups(Points<? extends Rollup> input) throws IOException {
        final HistogramRollup histogramRollup = new HistogramRollup(MAX_BIN_SIZE);
        histogramRollup.computeFromRollups(input);

        return histogramRollup;
    }

    public int getNumberOfBins() {
        return histogram.getMaxBins();
    }

    public Collection<Bin<SimpleTarget>> getBins() {
        return histogram.getBins();
    }

    public HashMap<Double, Double> getPercentile(Double... percentileLimit) {
        return histogram.percentiles(percentileLimit);
    }

    public static double getVariance(Points<SimpleNumber> input) {
        final Variance variance = new Variance();
        for (Map.Entry<Long, Points.Point<SimpleNumber>> item : input.getPoints().entrySet()) {
            variance.handleFullResMetric(item.getValue().getData().getValue());
        }

        return variance.toDouble();
    }

    public static int getIdealNumberOfBins(Points<SimpleNumber> input) {
        // Scott's rule
        return (int) Math.floor(3.5 * (Math.sqrt(getVariance(input))/Math.cbrt(input.getPoints().size())));
    }

    private double toDouble(Object val) throws RuntimeException {
        if (val instanceof Integer) {
            return new Double((Integer) val);
        } else if (val instanceof Long) {
            return new Double((Long) val);
        } else if (val instanceof Double) {
            return (Double) val;
        } else {
            throw new RuntimeException("Unsupported data type for histogram");
        }
    }
}
