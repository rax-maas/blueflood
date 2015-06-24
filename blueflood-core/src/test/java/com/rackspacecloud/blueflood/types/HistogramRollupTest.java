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

import com.bigml.histogram.Bin;
import com.bigml.histogram.SimpleTarget;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class HistogramRollupTest {

    @Test
    public void testSimpleHistogramFromRawSamples() throws Exception {
        Points<SimpleNumber> points = new Points<SimpleNumber>();
        long startTime = 12345678L;

        for (double val : TestData.DOUBLE_SRC) {
            points.add(new Points.Point<SimpleNumber>(startTime++, new SimpleNumber(val)));
        }

        HistogramRollup histogramRollup = HistogramRollup.buildRollupFromRawSamples(points);
        Assert.assertTrue(histogramRollup.getBins().size() <= HistogramRollup.MAX_BIN_SIZE);

        double count = 0;
        for (Bin<SimpleTarget> bin :histogramRollup.getBins()) {
            count += bin.getCount();
        }

        Assert.assertEquals(TestData.DOUBLE_SRC.length, (int) count);
    }

    @Test
    public void testMergeHistogramRollups() throws Exception {
        long startTime = 12345678L;
        int sampleSize = 10;
        Random rand = new Random();

        List<Points<SimpleNumber>> pointsList = new ArrayList<Points<SimpleNumber>>();
        Points<SimpleNumber> points = new Points<SimpleNumber>();
        pointsList.add(points);

        for (int i = 0; i < TestData.DOUBLE_SRC.length; i++) {
            if (i > 0 && (i % sampleSize) == 0) {
                points = new Points<SimpleNumber>();
                pointsList.add(points);
            }

            points.add(new Points.Point<SimpleNumber>(startTime + i, new SimpleNumber(TestData.DOUBLE_SRC[i])));
        }

        List<HistogramRollup> histogramRollups = new ArrayList<HistogramRollup>();
        for (Points<SimpleNumber> item : pointsList) {
            HistogramRollup histogramRollup = HistogramRollup.buildRollupFromRawSamples(item);
            histogramRollups.add(histogramRollup);
        }

        // Assert that there is more than 1 histogram rollup to test merging.
        Assert.assertTrue(histogramRollups.size() > 1);

        int first = rand.nextInt(histogramRollups.size());
        int second = rand.nextInt(histogramRollups.size());
        while (second == first) {
            second = rand.nextInt(histogramRollups.size());
        }

        Points<HistogramRollup> rollups = new Points<HistogramRollup>();
        rollups.add(new Points.Point<HistogramRollup>(startTime, histogramRollups.get(first)));
        rollups.add(new Points.Point<HistogramRollup>(startTime + 1, histogramRollups.get(second)));
        HistogramRollup merged = HistogramRollup.buildRollupFromRollups(rollups);

        Assert.assertTrue(merged.getBins().size() <= histogramRollups.get(first).getBins().size() +
                histogramRollups.get(second).getBins().size());
    }
}
