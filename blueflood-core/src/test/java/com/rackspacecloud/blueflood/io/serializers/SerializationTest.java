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

import com.google.protobuf.CodedOutputStream;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.io.serializers.metrics.BasicRollupSerDes;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.MetricHelper;
import com.google.common.collect.Sets;
import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

public class SerializationTest {

    protected static final double EPSILON = .5;

    private final static Object[] TO_SERIALIZE_FULL = new Object[] {
        32342341,
        3423523122452312341L,
        6345232.6234262d,
    };
    
    private static final Class[] SERIALIZABLE_TYPES = new Class[] {
            BasicRollup.class,
            SimpleNumber.class,
            Object.class,
            Integer.class,
            Long.class,
            BluefloodTimerRollup.class,
            BluefloodEnumRollup.class,
            BluefloodCounterRollup.class,
            BluefloodSetRollup.class,
            BluefloodGaugeRollup.class
    };
    
    private final static BasicRollup[] TO_SERIALIZE_BASIC_ROLLUP = new BasicRollup[4];

    static {
        // double
        for (int i = 0; i < 2; i++) {
            Points<SimpleNumber> input = new Points<SimpleNumber>();
            int timeOffset = 0;
            for (double val = 0.0; val < 10.0; val++) {
                input.add(new Points.Point<SimpleNumber>(123456789L + timeOffset++, new SimpleNumber(val * (i+1))));
            }

            try {
                TO_SERIALIZE_BASIC_ROLLUP[i] = BasicRollup.buildRollupFromRawSamples(input);
            } catch (IOException ex) {
                Assert.fail("Test data generation failed");
            }
        }

        // long
        for (int i = 0; i < 2; i++) {
            Points<SimpleNumber> input = new Points<SimpleNumber>();
            int timeOffset = 0;
            for (long val = 0; val < 10; val++) {
                input.add(new Points.Point<SimpleNumber>(123456789L + timeOffset++, new SimpleNumber(val * (i+1))));
            }
            try {
                TO_SERIALIZE_BASIC_ROLLUP[2 + i] = BasicRollup.buildRollupFromRawSamples(input);
            } catch (Exception e) {
                Assert.fail("Test data generation failed");
            }
        }
    }

    @Test
    public void testBadSerializationVersion() {
        byte[] buf = new byte[] {99, 99};  // hopefully we won't have 99 different serialization versions.
        for (Class type : SERIALIZABLE_TYPES) {
            try {
                Serializers.serializerFor(type).fromByteBuffer(ByteBuffer.wrap(buf));
                Assert.fail(String.format("Should have errored out %s", type.getName()));
            } catch (RuntimeException ex) {
                Throwable cause = ex.getCause();
                Assert.assertTrue("Serialization should fail for type: " + type, cause instanceof SerializationException);
            }
        }
    }
    
    @Test(expected = SerializationException.class)
    public void testVersion2FullDeserializeBadType() throws Throwable {
        byte[] buf = new byte[] { 0, 2 };
        try {
            Serializers.serializerFor(Object.class).fromByteBuffer(ByteBuffer.wrap(buf));
        } catch (RuntimeException e) {
            throw e.getCause();
        }
    }

    @Test
    public void testFullResSerializationAndDeserializationV1() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (Object o : TO_SERIALIZE_FULL) {
            // encode as base64 to make reading the output easier.
            baos.write(Base64.encodeBase64(Serializers.serializerFor(Object.class).toByteBuffer(o).array()));
            baos.write("\n".getBytes());
        }
        baos.close();

        BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(baos.toByteArray())));
        for (int i = 0; i < TO_SERIALIZE_FULL.length; i++) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
            Assert.assertEquals(
                        String.format("Deserializing broken for TO_SERIALIZE_FULL[%d]=%s", i, TO_SERIALIZE_FULL[i].toString()),
                        Serializers.serializerFor(Object.class).fromByteBuffer(byteBuffer),
                        TO_SERIALIZE_FULL[i]);
        }
        
        // ensure that current round-tripping isn't broken.
        for (Object o : TO_SERIALIZE_FULL) {
            ByteBuffer serialized = Serializers.serializerFor(Object.class).toByteBuffer(o);
            Assert.assertEquals(o, Serializers.serializerFor(Object.class).fromByteBuffer(serialized));
        }
    }

    @Test
    public void testBasicRollupSerializationAndDeserializationV1() throws IOException {

        BasicRollupSerDes serDes = new BasicRollupSerDes();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (BasicRollup basicRollup : TO_SERIALIZE_BASIC_ROLLUP) {
            ByteBuffer bb = serDes.serializeV1( basicRollup );
            baos.write(Base64.encodeBase64(bb.array()));
            baos.write("\n".getBytes());
        }
        baos.close();

        BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(baos.toByteArray())));
        for (int i = 0; i < TO_SERIALIZE_BASIC_ROLLUP.length; i++) {
            ByteBuffer bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
            BasicRollup basicRollup = Serializers.serializerFor(BasicRollup.class).fromByteBuffer(bb);

            // assert all sub-metrics are equal but sum, which is 0 for V1 rollups
            Assert.assertEquals( "average is same after serialization for V1", TO_SERIALIZE_BASIC_ROLLUP[i].getAverage(), basicRollup.getAverage() );
            Assert.assertEquals( "count is same after serialization for V1", TO_SERIALIZE_BASIC_ROLLUP[i].getCount(), basicRollup.getCount() );
            Assert.assertEquals( "max is same after serialization for V1", TO_SERIALIZE_BASIC_ROLLUP[i].getMaxValue(), basicRollup.getMaxValue() );
            Assert.assertEquals( "min is same after serialization for V1", TO_SERIALIZE_BASIC_ROLLUP[i].getMinValue(), basicRollup.getMinValue() );
            Assert.assertEquals( "var is same after serialization for V1", TO_SERIALIZE_BASIC_ROLLUP[i].getVariance(), basicRollup.getVariance() );
            Assert.assertEquals( "sum is 0 after serialization for V1", 0, basicRollup.getSum(), EPSILON );
        }
    }

    @Test
    public void testBasicRollupSerializationAndDeserialization() throws IOException {
        // works the same way as testFullResSerializationAndDeserialization

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (BasicRollup basicRollup : TO_SERIALIZE_BASIC_ROLLUP) {
            ByteBuffer bb = Serializers.serializerFor(BasicRollup.class).toByteBuffer(basicRollup);
            baos.write(Base64.encodeBase64(bb.array()));
            baos.write("\n".getBytes());
        }
        baos.close();

        BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(baos.toByteArray())));
        for (int i = 0; i < TO_SERIALIZE_BASIC_ROLLUP.length; i++) {
            ByteBuffer bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
            BasicRollup basicRollup = Serializers.serializerFor(BasicRollup.class).fromByteBuffer(bb);
            Assert.assertTrue(String.format("Deserialization for rollup TO_SERIALIZE_BASIC_ROLLUP[%d]=%s",
                                            i, TO_SERIALIZE_BASIC_ROLLUP[i]),
                                TO_SERIALIZE_BASIC_ROLLUP[i].equals(basicRollup));
        }
        
        // current round tripping.
        for (BasicRollup basicRollup : TO_SERIALIZE_BASIC_ROLLUP) {
            ByteBuffer bb = Serializers.serializerFor(BasicRollup.class).toByteBuffer(basicRollup);
            Assert.assertTrue(basicRollup.equals(Serializers.serializerFor(BasicRollup.class).fromByteBuffer(bb)));
        }
    }

    @Test
    public void testFullResRoundTrip() throws IOException {
        // tests serialization of all types that should be handled, including granularity variations.
        Object[] inputs = {
            7565,
            323234234235223321L,
            213432.53323d,
            42332.0234375f,
            TO_SERIALIZE_BASIC_ROLLUP[0],
            TO_SERIALIZE_BASIC_ROLLUP[1],
            TO_SERIALIZE_BASIC_ROLLUP[2],
            TO_SERIALIZE_BASIC_ROLLUP[3]
        };
        
        Object[] expected = {
            7565,
            323234234235223321L,
            213432.53323d,
            42332.0234375d, // notice that serialization converts to a double.
            TO_SERIALIZE_BASIC_ROLLUP[0],
            TO_SERIALIZE_BASIC_ROLLUP[1],
            TO_SERIALIZE_BASIC_ROLLUP[2],
            TO_SERIALIZE_BASIC_ROLLUP[3]
        };
        
        for (Class type : SERIALIZABLE_TYPES) {
            for (int i = 0; i < inputs.length; i++) {
                try {
                    Object dst = Serializers.serializerFor(type).fromByteBuffer(Serializers.serializerFor(type).toByteBuffer(inputs[i]));
                    Assert.assertEquals(String.format("busted at %s %d", type.getName(), i), expected[i], dst);
                } catch (ClassCastException ex) {
                    // these are expected because of the various types.
                    // todo: this test could be made better by
                    //       1) having one iteration to verify that we can serialize for matched types.
                    //       2) having various other tests that verify that serialization breaks in expected ways when
                    //          types are mismatched
                    continue;
                } catch (RuntimeException ex) {
                    if (ex.getCause() == null) throw ex;
                    Assert.assertTrue(ex.getCause().getClass().getName(), ex.getCause() instanceof SerializationException);
                    if (inputs[i] instanceof BasicRollup)
                        Assert.assertFalse(type.equals(BasicRollup.class));
                    else
                        Assert.assertTrue(type.equals(BasicRollup.class));
                } catch (Throwable unexpected) {
                    unexpected.printStackTrace();
                    Assert.fail(String.format("Unexpected error at %s %d", type.getName(), i));
                }
            }
        }
    }

    @Test
    public void testSerializerOverAndOver() throws IOException {
        byte[] buf;
        int expectedBufferSize = 0;
        for (int i = 0; i < 10000000; i++) {
            buf = Serializers.serializerFor(Long.class).toByteBuffer(Long.MAX_VALUE).array();
            Assert.assertFalse(buf.length == 0);
            if (expectedBufferSize == 0)
                expectedBufferSize = buf.length;
            else
              Assert.assertEquals(buf.length, expectedBufferSize);
        }
    }

    @Test(expected = SerializationException.class)
    public void testCannotRoundtripBytesWillNullType() throws Throwable {
        try {
            byte[] expected = new byte[] {1,2,3,4,5};
            ByteBuffer bb = Serializers.serializerFor((Class) null).toByteBuffer(expected);
            byte[] actual = (byte[]) Serializers.serializerFor((Class) null).fromByteBuffer(bb);
            Assert.assertArrayEquals(expected, actual);
        } catch (RuntimeException ex) {
            throw ex.getCause();
        }
    }
    
    @Test(expected = SerializationException.class)
    public void testCannotRoundtripBytes() throws Throwable {
        try {
            byte[] expected = new byte[] {1,2,3,4,5};
            AbstractSerializer ser = Serializers.serializerFor(SimpleNumber.class);
            byte[] actual = (byte[])ser.fromByteBuffer(ser.toByteBuffer(expected));
            Assert.assertArrayEquals(expected, actual);
        } catch (RuntimeException ex) {
            throw ex.getCause();
        }
    }

    @Test
    public void testForConstantCollisions() throws Exception {
        // make sure we're not sharing any constants with MetricHelper.DataType
        Set<Character> metricHelperTypes = new HashSet<Character>();
        for (Field f : MetricHelper.Type.class.getFields())
            if (f.getType().equals(char.class))
                metricHelperTypes.add(((Character)f.get(MetricHelper.Type.class)));
        Assert.assertEquals(7, metricHelperTypes.size());

        Set<Character> serializerTypes = new HashSet<Character>();
        for (Field f : Serializers.Type.class.getDeclaredFields())
            if (f.getType().equals(byte.class))
                serializerTypes.add((char)((Byte)f.get(MetricHelper.Type.class)).byteValue());
        Assert.assertEquals(8, serializerTypes.size());

        // intersection should be zero.
        Assert.assertEquals(0, Sets.intersection(metricHelperTypes, serializerTypes).size());

        // so that I know Sets.intersection is not making a fool of me.
        serializerTypes.add(metricHelperTypes.iterator().next());
        Assert.assertEquals(1, Sets.intersection(metricHelperTypes, serializerTypes).size());
    }
}
