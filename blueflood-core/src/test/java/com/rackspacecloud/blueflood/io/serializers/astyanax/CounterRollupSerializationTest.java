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
import com.rackspacecloud.blueflood.types.BluefloodCounterRollup;
import junit.framework.Assert;
import org.apache.commons.codec.binary.Base64;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;

public class CounterRollupSerializationTest {

    @Test
    public void testCounterV1RoundTrip() throws IOException {
        BluefloodCounterRollup c0 = new BluefloodCounterRollup().withCount(7442245).withSampleCount(1);
        BluefloodCounterRollup c1 = new BluefloodCounterRollup().withCount(34454722343L).withSampleCount(10);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        baos.write(Base64.encodeBase64(Serializers.counterRollupInstance.toByteBuffer(c0).array()));
        baos.write("\n".getBytes());
        baos.write(Base64.encodeBase64(Serializers.counterRollupInstance.toByteBuffer(c1).array()));
        baos.write("\n".getBytes());

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        BufferedReader reader = new BufferedReader(new InputStreamReader(bais));

        ByteBuffer bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
        BluefloodCounterRollup cc0 = Serializers.serializerFor(BluefloodCounterRollup.class).fromByteBuffer(bb);
        Assert.assertEquals(c0, cc0);

        bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
        BluefloodCounterRollup cc1 = Serializers.serializerFor(BluefloodCounterRollup.class).fromByteBuffer(bb);

        Assert.assertEquals(c1, cc1);
        Assert.assertFalse(cc0.equals(cc1));
    }
}
