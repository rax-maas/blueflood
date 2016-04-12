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

package com.rackspacecloud.blueflood.io.serializers.astyanax;

import com.rackspacecloud.blueflood.io.serializers.Serializers;
import com.rackspacecloud.blueflood.types.BluefloodTimerRollup;
import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
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

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        //The V1 serialization is artificially constructed for the purposes of this test and should no longer be used.
        baos.write(Base64.encodeBase64(Serializers.timerRollupInstance.toByteBufferWithV1Serialization(r0).array()));
        baos.write("\n".getBytes());
        baos.close();

        BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(baos.toByteArray())));
        ByteBuffer bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
        BluefloodTimerRollup r1 = Serializers.timerRollupInstance.fromByteBuffer(bb);
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

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write(Base64.encodeBase64(Serializers.timerRollupInstance.toByteBuffer(r0).array()));
        baos.write("\n".getBytes());
        baos.close();

        BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(baos.toByteArray())));
        ByteBuffer bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
        BluefloodTimerRollup r1 = Serializers.timerRollupInstance.fromByteBuffer(bb);
        Assert.assertEquals(r0, r1);
    }
}