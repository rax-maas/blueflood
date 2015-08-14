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

import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.types.BluefloodTimerRollup;
import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class TimerSerializationTest {

    @Test
    public void testV1RoundTrip() throws IOException {
        // build up a Timer
        BluefloodTimerRollup r0 = new BluefloodTimerRollup()
                .withSum(Double.valueOf(42))
                .withCountPS(23.32d)
                .withAverage(56)
                .withVariance(853.3245d)
                .withMinValue(2)
                .withMaxValue(987)
                .withCount(345);
        r0.setPercentile("foo", 741.32d);
        r0.setPercentile("bar", 0.0323d);

        if (System.getProperty("GENERATE_TIMER_SERIALIZATION") != null) {
            OutputStream os = new FileOutputStream("src/test/resources/serializations/timer_version_" + Constants.VERSION_1_TIMER + ".bin", false);
            //The V1 serialization is artificially constructed for the purposes of this test and should no longer be used.
            os.write(Base64.encodeBase64(new NumericSerializer.TimerRollupSerializer().toByteBufferWithV1Serialization(r0).array()));
            os.write("\n".getBytes());
            os.close();
        }

        Assert.assertTrue(new File("src/test/resources/serializations").exists());

        int version = 0;

        BufferedReader reader = new BufferedReader(new FileReader("src/test/resources/serializations/timer_version_" + version + ".bin"));
        ByteBuffer bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
        BluefloodTimerRollup r1 = new NumericSerializer.TimerRollupSerializer().fromByteBuffer(bb);
        Assert.assertEquals(r0, r1);
    }

    @Test
    public void testV2RoundTrip() throws IOException {
        // build up a Timer
        BluefloodTimerRollup r0 = new BluefloodTimerRollup()
                .withSum(Double.valueOf(42))
                .withCountPS(23.32d)
                .withAverage(56)
                .withVariance(853.3245d)
                .withMinValue(2)
                .withMaxValue(987)
                .withCount(345);
        r0.setPercentile("foo", 741.32d);
        r0.setPercentile("bar", 0.0323d);

        if (System.getProperty("GENERATE_TIMER_SERIALIZATION") != null) {
            OutputStream os = new FileOutputStream("src/test/resources/serializations/timer_version_" + Constants.VERSION_2_TIMER + ".bin", false);
            os.write(Base64.encodeBase64(new NumericSerializer.TimerRollupSerializer().toByteBuffer(r0).array()));
            os.write("\n".getBytes());
            os.close();
        }

        Assert.assertTrue(new File("src/test/resources/serializations").exists());

        // ensure historical reads work.
        int version = 0;
        int maxVersion = Constants.VERSION_2_TIMER;

        int count = 0;
        while (version <= maxVersion) {
            BufferedReader reader = new BufferedReader(new FileReader("src/test/resources/serializations/timer_version_" + version + ".bin"));
            ByteBuffer bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
            BluefloodTimerRollup r1 = new NumericSerializer.TimerRollupSerializer().fromByteBuffer(bb);
            Assert.assertEquals(r0, r1);
            count++;
            version++;
        }

        Assert.assertTrue("Nothing was tested", count > 0);
    }
}