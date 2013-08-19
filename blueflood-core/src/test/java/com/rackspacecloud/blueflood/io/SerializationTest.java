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

package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.exceptions.UnexpectedStringSerializationException;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Rollup;
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
    
    private final static Object[] toSerializeFull = new Object[] {
        32342341,
        3423523122452312341L,
        6345232.6234262d,
        "This is a test string."
    };
    
    private final static Rollup[] toSerializeRollup = new Rollup[4];

    static {
        Rollup rollup;
        // double
        for (int i = 0; i < 2; i++) {
            rollup = new Rollup();
            for (double val = 0.0; val < 10.0; val++) {
                try {
                    rollup.handleFullResMetric(val * (i+1));
                } catch (Exception e) {
                    Assert.fail("Test data generation failed");
                }
            }
            toSerializeRollup[i] = rollup;
        }

        // long
        for (int i = 0; i < 2; i++) {
            rollup = new Rollup();
            for (long val = 0; val < 10; val++) {
                try {
                    rollup.handleFullResMetric(val * (i+1));
                } catch (Exception e) {
                    Assert.fail("Test data generation failed");
                }
            }
            toSerializeRollup[2 + i] = rollup;
        }
    }

    @Test
    public void testBadSerializationVersion() {
        byte[] buf = new byte[] {99, 99};  // hopefully we won't have 99 different serialization versions.
        for (Granularity g : Granularity.granularities()) {
            try {
                NumericSerializer.get(g).fromByteBuffer(ByteBuffer.wrap(buf));
                Assert.fail(String.format("Should have errored out %s", g.name()));
            } catch (RuntimeException ex) {
                Assert.assertTrue(ex.getCause().getMessage().startsWith("Unexpected serialization version"));
            }
        }
    }
    
    @Test(expected = SerializationException.class)
    public void testVersion2FullDeserializeBadType() throws Throwable {
        byte[] buf = new byte[] { 0, 2 };
        try {
            NumericSerializer.get(Granularity.FULL).fromByteBuffer(ByteBuffer.wrap(buf));
        } catch (RuntimeException e) {
            throw e.getCause();
        }
    }

    @Test
    public void testFullResSerializationAndDeserialization() throws IOException {
        // if the GENERATE_SERIALIZATION flag is set, save everything.
        if (System.getProperty("GENERATE_FULL_RES_SERIALIZATION") != null) {
            OutputStream os = new FileOutputStream("src/test/resources/serializations/full_version_" + Constants.VERSION_1_FULL_RES + ".bin", false);
            for (Object o : toSerializeFull) {
                // encode as base64 to make reading the file easier.

                os.write(Base64.encodeBase64(NumericSerializer.get(Granularity.FULL).toByteBuffer(o).array()));
                os.write("\n".getBytes());
            }
            os.close();
        }
        
        Assert.assertTrue(new File("src/test/resources/serializations").exists());
        
        // ensure we can read historical serializations.
        int version = 0; // versions before this are illegal.
        int maxVersion = Constants.VERSION_1_FULL_RES;
        while (version <= maxVersion) {
            BufferedReader reader = new BufferedReader(new FileReader("src/test/resources/serializations/full_version_" + version + ".bin"));
            for (int i = 0; i < toSerializeFull.length; i++)
                try {
                    // we used to allow deserializing strings, but we don't anymore.
                    // catch that error and assert it happens only when expected.
                    ByteBuffer byteBuffer = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
                    Assert.assertEquals(
                            String.format("broken at version %d", version),
                            NumericSerializer.get(Granularity.FULL).fromByteBuffer(byteBuffer),
                            toSerializeFull[i]);
                } catch (RuntimeException ex) {
                    Assert.assertEquals(ex.getCause().getClass(), UnexpectedStringSerializationException.class);
                    Assert.assertEquals(3, i);
                    Assert.assertTrue(toSerializeFull[i] instanceof String);
                }
            version += 1;
        }
        
        // ensure that current round-tripping isn't broken.
        for (Object o : toSerializeFull) {
            // skip the string (we used to allow this).
            if (o instanceof String) continue; // we don't serialize those any more.
            ByteBuffer serialized = NumericSerializer.get(Granularity.FULL).toByteBuffer(o);
            Assert.assertEquals(o, NumericSerializer.get(Granularity.FULL).fromByteBuffer(serialized));
        }
    }

    @Test
    public void testRollupSerializationAndDeserialization() throws IOException {
        // works the same way as testFullResSerializationAndDeserialization
        
        if (System.getProperty("GENERATE_ROLLUP_SERIALIZATION") != null) {
            OutputStream os = new FileOutputStream("src/test/resources/serializations/rollup_version_" + Constants.VERSION_1_ROLLUP + ".bin", false);
            for (Rollup rollup : toSerializeRollup) {
                for (Granularity g : Granularity.rollupGranularities()) {
                    ByteBuffer bb = NumericSerializer.get(g).toByteBuffer(rollup);
                    os.write(Base64.encodeBase64(bb.array()));
                    os.write("\n".getBytes());
                }
            }
            os.close();
        }
        
        Assert.assertTrue(new File("src/test/resources/serializations").exists());
        
        // ensure we can read historical serializations.
        int version = 0;
        int maxVersion = Constants.VERSION_1_ROLLUP;
        while (version <= maxVersion) {
            BufferedReader reader = new BufferedReader(new FileReader("src/test/resources/serializations/rollup_version_" + version + ".bin"));
            for (int i = 0; i < toSerializeRollup.length; i++) {
                for (Granularity g : Granularity.rollupGranularities()) {
                    ByteBuffer bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
                    Rollup rollup = (Rollup)NumericSerializer.get(g).fromByteBuffer(bb);
                    Assert.assertTrue(String.format("Deserialization for rollup broken at %d", version),
                            toSerializeRollup[i].equals(rollup));
                }
                version += 1;
            }
        }
        
        // current round tripping.
        for (Rollup rollup : toSerializeRollup) {
            for (Granularity g : Granularity.rollupGranularities()) {
                ByteBuffer bb = NumericSerializer.get(g).toByteBuffer(rollup);
                Assert.assertTrue(rollup.equals(NumericSerializer.get(g).fromByteBuffer(bb)));
            }
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
            toSerializeRollup[0],
            toSerializeRollup[1],
            toSerializeRollup[2],
            toSerializeRollup[3]
        };
        
        Object[] expected = {
            7565,
            323234234235223321L,
            213432.53323d,
            42332.0234375d, // notice that serialization converts to a double.
            toSerializeRollup[0],
            toSerializeRollup[1],
            toSerializeRollup[2],
            toSerializeRollup[3]
        };
        
        for (Granularity gran : Granularity.granularities()) {
            for (int i = 0; i < inputs.length; i++) {
                try {
                    Object dst = NumericSerializer.get(gran).fromByteBuffer(NumericSerializer.get(gran).toByteBuffer(inputs[i]));
                    Assert.assertEquals(String.format("busted at %s %d", gran.name(), i), expected[i], dst);
                } catch (ClassCastException ex) {
                    ex.printStackTrace();
                    Assert.fail("Serialization should address class type mismatches as IOExceptions");
                } catch (RuntimeException ex) {
                    Assert.assertTrue(ex.getCause() instanceof SerializationException);
                    if (gran == Granularity.FULL)
                        Assert.assertTrue(inputs[i] instanceof Rollup);
                    else
                        Assert.assertFalse(inputs[i] instanceof Rollup);
                } catch (Throwable unexpected) {
                    unexpected.printStackTrace();
                    Assert.fail(String.format("Unexpected error at %s %d", gran.name(), i));
                }
            }
        }
    }

    @Test
    public void testSerializerOverAndOver() throws IOException {
        byte[] buf;
        int expectedBufferSize = 0;
        for (int i = 0; i < 10000000; i++) {
            buf = NumericSerializer.get(Granularity.FULL).toByteBuffer(Long.MAX_VALUE).array();
            Assert.assertFalse(buf.length == 0);
            if (expectedBufferSize == 0)
                expectedBufferSize = buf.length;
            else
              Assert.assertEquals(buf.length, expectedBufferSize);
        }
    }
    
    @Test(expected = SerializationException.class)
    public void testSerializeStringFails() throws Throwable {
        try {
            NumericSerializer.get(Granularity.FULL).toByteBuffer("words");
        } catch (RuntimeException e) {
            throw e.getCause();
        }
    }
    
    @Test(expected = UnexpectedStringSerializationException.class)
    public void testDeserializeStringDoesNotFail() throws Throwable {
        // this is what a string looked like previously.
        try {
            String serialized = "AHMWVGhpcyBpcyBhIHRlc3Qgc3RyaW5nLg==";
            ByteBuffer bb = ByteBuffer.wrap(Base64.decodeBase64(serialized.getBytes()));
            NumericSerializer.get(Granularity.FULL).fromByteBuffer(bb);
        } catch (RuntimeException ex) {
            throw ex.getCause();
        }
    }
    
    // this was allowed for a brief while. it would represent a regression now.
    @Test(expected = SerializationException.class)
    public void testCannotRoundtripStringWithNullGran() throws Throwable {
        try {
            String expected = "this is a string";
            Granularity gran = null;
            ByteBuffer bb = NumericSerializer.get(gran).toByteBuffer(expected);
            String actual = (String)NumericSerializer.get(gran).fromByteBuffer(bb);
            Assert.assertEquals(expected, actual);
        } catch (RuntimeException ex) {
            throw ex.getCause();
        }
    }
    
    @Test(expected = SerializationException.class)
    public void testCannotRoundtripBytesWillNullGran() throws Throwable {
        try {
            byte[] expected = new byte[] {1,2,3,4,5};
            Granularity gran = null;
            ByteBuffer bb = NumericSerializer.get(gran).toByteBuffer(expected);
            byte[] actual = (byte[])NumericSerializer.get(gran).fromByteBuffer(bb);
            Assert.assertArrayEquals(expected, actual);
        } catch (RuntimeException ex) {
            throw ex.getCause();
        }
    }
    
    @Test(expected = SerializationException.class)
    public void testCannotRoundtripBytes() throws Throwable {
        try {
            byte[] expected = new byte[] {1,2,3,4,5};
            Granularity gran = Granularity.FULL;
            NumericSerializer ser = NumericSerializer.get(gran);
            byte[] actual = (byte[])ser.fromByteBuffer(ser.toByteBuffer(expected));
            Assert.assertArrayEquals(expected, actual);
        } catch (RuntimeException ex) {
            throw ex.getCause();
        }
    }
  
    @Test
    public void testForConstantCollisions() throws Exception {
        // make sure we're not sharing any constants with MetricHelper.Type
        Set<Character> metricHelperTypes = new HashSet<Character>();
        for (Field f : MetricHelper.Type.class.getFields())
            if (f.getType().equals(char.class))
                metricHelperTypes.add(((Character)f.get(MetricHelper.Type.class)));
        Assert.assertEquals(7, metricHelperTypes.size());
        
        Set<Character> serializerTypes = new HashSet<Character>();
        for (Field f : NumericSerializer.Type.class.getDeclaredFields())
            if (f.getType().equals(byte.class))
                serializerTypes.add((char)((Byte)f.get(MetricHelper.Type.class)).byteValue());
        Assert.assertEquals(3, serializerTypes.size());

        // intersection should be zero.
        Assert.assertEquals(0, Sets.intersection(metricHelperTypes, serializerTypes).size());
        
        // so that I know Sets.intersection is not making a fool of me.
        serializerTypes.add(metricHelperTypes.iterator().next());
        Assert.assertEquals(1, Sets.intersection(metricHelperTypes, serializerTypes).size());
    }
  
    @Test
    public void testRollupSerializationLargeCounts() throws IOException {
        Rollup r = new Rollup();
        r.setCount(500);
        for (int rollupCount = 0; rollupCount < 500; rollupCount++) {
            Rollup rollup = new Rollup();
            for (int fullResCount = 0; fullResCount < 500; fullResCount++) {
                rollup.handleFullResMetric(fullResCount + fullResCount * 3);
            }
            r.handleRollupMetric(rollup);
        }
        // serialization was broken.
        ByteBuffer bb = NumericSerializer.get(Granularity.MIN_240).toByteBuffer(r);
        Assert.assertEquals(r, NumericSerializer.get(Granularity.MIN_240).fromByteBuffer(bb));
    }

    @Test
    public void testLocatorDeserializer() throws UnsupportedEncodingException {
        String locatorString = "ac76PeGPSR,entZ4MYd1W,chJ0fvB5Ao,mzord.truncated";
        ByteBuffer bb = ByteBuffer.wrap(locatorString.getBytes("UTF-8"));
        Locator locatorFromString = Locator.createLocatorFromDbKey(locatorString);
        Locator locatorDeserialized = LocatorSerializer.get().fromByteBuffer(bb);
        Assert.assertEquals("Locator did not match after deserialization",
                locatorFromString.toString(), locatorDeserialized.toString());
    }
}