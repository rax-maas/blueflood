/*
 * Copyright 2015 Rackspace
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
import com.rackspacecloud.blueflood.types.BluefloodEnumRollup;
import com.rackspacecloud.blueflood.types.BluefloodEnumRollupTest;
import com.rackspacecloud.blueflood.types.Points;
import junit.framework.Assert;
import org.apache.commons.codec.binary.Base64;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Map;

public class EnumRollupSerializationTest {

    @Test
    public void testEnumV1RoundTrip() throws IOException {
        BluefloodEnumRollup e0 = new BluefloodEnumRollup().withEnumValue("enumValue1", 1L).withEnumValue("enumValue2", 5L);
        BluefloodEnumRollup e1 = new BluefloodEnumRollup().withEnumValue("t4.enum.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met", 34454722343L)
                            .withEnumValue("t5.enum.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met", 34454722343L)
                            .withEnumValue("enumValue2",10L);
        BluefloodEnumRollup e2 = new BluefloodEnumRollup().withEnumValue("enumValue1",1L).withEnumValue("enumValue1",1L);
        BluefloodEnumRollup er = BluefloodEnumRollup.buildRollupFromEnumRollups(BluefloodEnumRollupTest.asPoints(BluefloodEnumRollup.class, 0, 300, e0, e1, e2));
        Assert.assertEquals(4, er.getCount());
        Map<Long, Long> map = er.getHashedEnumValuesWithCounts();
        Assert.assertTrue(map.get((long)"enumValue1".hashCode()) == 3L);
        Assert.assertTrue(map.get((long)"enumValue2".hashCode()) == 15L);

        if (System.getProperty("GENERATE_ENUM_SERIALIZATION") != null) {
            OutputStream os = new FileOutputStream("src/test/resources/serializations/enum_version_" + Constants.VERSION_1_ENUM_ROLLUP + ".bin", false);
            os.write(Base64.encodeBase64(new NumericSerializer.EnumRollupSerializer().toByteBuffer(e0).array()));
            os.write("\n".getBytes());
            os.write(Base64.encodeBase64(new NumericSerializer.EnumRollupSerializer().toByteBuffer(e1).array()));
            os.write("\n".getBytes());
            os.write(Base64.encodeBase64(new NumericSerializer.EnumRollupSerializer().toByteBuffer(e2).array()));
            os.write("\n".getBytes());
            os.write(Base64.encodeBase64(new NumericSerializer.EnumRollupSerializer().toByteBuffer(er).array()));
            os.write("\n".getBytes());
            os.close();
        }

        Assert.assertTrue(new File("src/test/resources/serializations").exists());

        int count = 0;
        int version = 0;
        final int maxVersion = Constants.VERSION_1_ENUM_ROLLUP;
        while (version <= maxVersion) {
            BufferedReader reader = new BufferedReader(new FileReader("src/test/resources/serializations/enum_version_" + version + ".bin"));

            ByteBuffer bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
            BluefloodEnumRollup ee0 = NumericSerializer.serializerFor(BluefloodEnumRollup.class).fromByteBuffer(bb);
            Assert.assertEquals(e0, ee0);

            bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
            BluefloodEnumRollup ee1 = NumericSerializer.serializerFor(BluefloodEnumRollup.class).fromByteBuffer(bb);
            Assert.assertEquals(e1, ee1);

            bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
            BluefloodEnumRollup ee2 = NumericSerializer.serializerFor(BluefloodEnumRollup.class).fromByteBuffer(bb);
            Assert.assertEquals(e2, ee2);

            bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
            BluefloodEnumRollup ee3 = NumericSerializer.serializerFor(BluefloodEnumRollup.class).fromByteBuffer(bb);
            Assert.assertEquals(er, ee3);

            Assert.assertFalse(ee0.equals(ee1));
            version++;
            count++;
        }

        Assert.assertTrue(count > 0);
    }
}
