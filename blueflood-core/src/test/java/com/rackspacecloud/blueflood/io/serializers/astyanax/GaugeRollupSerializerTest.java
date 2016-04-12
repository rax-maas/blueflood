/*
 * Copyright 2016 Rackspace
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
import com.rackspacecloud.blueflood.types.BluefloodGaugeRollup;
import com.rackspacecloud.blueflood.types.SimpleNumber;
import com.rackspacecloud.blueflood.utils.Rollups;
import junit.framework.Assert;
import org.apache.commons.codec.binary.Base64;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

public class GaugeRollupSerializerTest {

    @Test
    public void testSerializerDeserializerV1() throws Exception {
        BluefloodGaugeRollup gauge1 = BluefloodGaugeRollup.buildFromRawSamples(
                                        Rollups.asPoints(SimpleNumber.class, System.currentTimeMillis(), 300, new SimpleNumber(10L)));
        BluefloodGaugeRollup gauge2 = BluefloodGaugeRollup.buildFromRawSamples(
                Rollups.asPoints(SimpleNumber.class, System.currentTimeMillis()-100, 300, new SimpleNumber(1234567L)));
        BluefloodGaugeRollup gauge3 = BluefloodGaugeRollup.buildFromRawSamples(
                Rollups.asPoints(SimpleNumber.class, System.currentTimeMillis()-200, 300, new SimpleNumber(10.4D)));
        BluefloodGaugeRollup gaugesRollup = BluefloodGaugeRollup.buildFromGaugeRollups(
                Rollups.asPoints(BluefloodGaugeRollup.class, System.currentTimeMillis(), 300, gauge1, gauge2, gauge3));
        Assert.assertEquals(3, gaugesRollup.getCount());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write(Base64.encodeBase64(Serializers.gaugeRollupInstance.toByteBuffer(gauge1).array()));
        baos.write("\n".getBytes());
        baos.write(Base64.encodeBase64(Serializers.gaugeRollupInstance.toByteBuffer(gauge2).array()));
        baos.write("\n".getBytes());
        baos.write(Base64.encodeBase64(Serializers.gaugeRollupInstance.toByteBuffer(gauge3).array()));
        baos.write("\n".getBytes());
        baos.write(Base64.encodeBase64(Serializers.gaugeRollupInstance.toByteBuffer(gaugesRollup).array()));
        baos.write("\n".getBytes());
        baos.close();

        BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(baos.toByteArray())));

        ByteBuffer bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
        BluefloodGaugeRollup deserializedGauge1 = Serializers.serializerFor(BluefloodGaugeRollup.class).fromByteBuffer(bb);
        Assert.assertEquals(gauge1, deserializedGauge1);

        bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
        BluefloodGaugeRollup deserializedGauge2 = Serializers.serializerFor(BluefloodGaugeRollup.class).fromByteBuffer(bb);
        Assert.assertEquals(gauge2, deserializedGauge2);

        bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
        BluefloodGaugeRollup deserializedGauge3 = Serializers.serializerFor(BluefloodGaugeRollup.class).fromByteBuffer(bb);
        Assert.assertEquals(gauge3, deserializedGauge3);

        bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
        BluefloodGaugeRollup deserializedGauge4 = Serializers.serializerFor(BluefloodGaugeRollup.class).fromByteBuffer(bb);
        Assert.assertEquals(gaugesRollup, deserializedGauge4);

        Assert.assertFalse(deserializedGauge1.equals(deserializedGauge2));
    }
}
