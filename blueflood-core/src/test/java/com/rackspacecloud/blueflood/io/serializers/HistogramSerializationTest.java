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

package com.rackspacecloud.blueflood.io.serializers;

import com.bigml.histogram.Bin;
import com.bigml.histogram.SimpleTarget;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.types.*;
import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

public class HistogramSerializationTest {
    private static HistogramRollup histogramRollup;

    static {
        Points<SimpleNumber> points = new Points<SimpleNumber>();
        long startTime = 12345678L;
        for (double val : TestData.DOUBLE_SRC) {
            points.add(new Points.Point<SimpleNumber>(startTime++, new SimpleNumber(val)));
        }

        try {
             histogramRollup = HistogramRollup.buildRollupFromRawSamples(points);
        } catch (Exception ex) {
            Assert.fail("Test data generation failed");
        }
    }

    @Test
    public void testSerializationDeserializationVersion1() throws Exception {
        if (System.getProperty("GENERATE_HIST_SERIALIZATION") != null) {
            OutputStream os = new FileOutputStream("src/test/resources/serializations/histogram_version_" +
                    Constants.VERSION_1_HISTOGRAM + ".bin", false);

            os.write(Base64.encodeBase64(HistogramSerializer.get().toByteBuffer(histogramRollup).array()));
            os.write("\n".getBytes());
            os.close();
        }

        Assert.assertTrue(new File("src/test/resources/serializations").exists());

        // ensure we can read historical serializations.
        int version = 0;
        int maxVersion = Constants.VERSION_1_HISTOGRAM;
        while (version <= maxVersion) {
            BufferedReader reader = new BufferedReader(new FileReader("src/test/resources/serializations/histogram_version_" + version + ".bin"));
            ByteBuffer bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
            HistogramRollup histogramRollupDes = HistogramSerializer.get().fromByteBuffer(bb);
            Assert.assertTrue(areHistogramsEqual(histogramRollup, histogramRollupDes));
            version++;
        }
    }

    @Test
    public void testBadSerializationVersion() {
        byte[] buf = new byte[] {99, 99};  // hopefully we won't have 99 different serialization versions.
        try {
            HistogramSerializer.get().fromByteBuffer(ByteBuffer.wrap(buf));
            Assert.fail(String.format("Should have errored out. Such a version doesn't exist for histogram."));
        } catch (RuntimeException ex) {
            Assert.assertTrue(ex.getCause().getMessage().startsWith("Unexpected serialization version"));
        }
    }

    private boolean areHistogramsEqual(HistogramRollup first, HistogramRollup second) {
        final TreeMap<Double, Double> firstBinsAsOrderedMap = getNonZeroBinsAsMap(first);
        final TreeMap<Double, Double> secondBinsAsOrderedMap = getNonZeroBinsAsMap(second);

        if (firstBinsAsOrderedMap.size() != secondBinsAsOrderedMap.size()) {
            return false;
        }

        for (Map.Entry<Double, Double> firstBin: firstBinsAsOrderedMap.entrySet()) {
            Double val = secondBinsAsOrderedMap.get(firstBin.getKey());
            if (val == null || !firstBin.getValue().equals(val)) {
                return false;
            }
        }

        return true;
    }

    private TreeMap<Double, Double> getNonZeroBinsAsMap(HistogramRollup histogramRollup) {
        Collection<Bin<SimpleTarget>> bins = histogramRollup.getBins();

        final TreeMap<Double, Double> binsMap = new TreeMap<Double, Double>();
        for (Bin<SimpleTarget> bin : bins) {
            if (bin.getCount() > 0) {
                binsMap.put(bin.getMean(), bin.getCount());
            }
        }

        return binsMap;
    }
}
