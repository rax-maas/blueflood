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

public class HistogramRollupTest {

    @Test
    public void testSimpleHistogramFromRawSamples() throws Exception {
        Points<SimpleNumber> points = new Points<SimpleNumber>();
        long startTime = 12345678L;

        for (double val : TestData.DOUBLE_SRC) {
            points.add(new Points.Point<SimpleNumber>(startTime++, new SimpleNumber(val)));
        }

        HistogramRollup histogramRollup = HistogramRollup.buildRollupFromRawSamples(points);
        Assert.assertTrue(histogramRollup.getNumberOfBins() <= HistogramRollup.MAX_BIN_SIZE);

        double count = 0;
        for (Bin<SimpleTarget> bin :histogramRollup.getBins()) {
            count += bin.getCount();
        }

        Assert.assertEquals(TestData.DOUBLE_SRC.length, (int) count);
    }
}
