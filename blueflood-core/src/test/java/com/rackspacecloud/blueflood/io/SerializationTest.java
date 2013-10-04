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

import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.exceptions.UnexpectedStringSerializationException;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Points;
import com.rackspacecloud.blueflood.types.Rollup;
import com.rackspacecloud.blueflood.types.SimpleNumber;
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
        for (Granularity g : Granularity.granularities()) {
            try {
                NumericSerializer.get(AstyanaxIO.getColumnFamilyMapper().get(g.name()))
                                 .fromByteBuffer(ByteBuffer.wrap(buf));
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
            NumericSerializer.get(AstyanaxIO.getColumnFamilyMapper().get(Granularity.FULL.name()))
                             .fromByteBuffer(ByteBuffer.wrap(buf));
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

                os.write(Base64.encodeBase64(
                        NumericSerializer.get(AstyanaxIO.getColumnFamilyMapper().get(Granularity.FULL.name()))
                                         .toByteBuffer(o).array()
                        )
                );
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
                            NumericSerializer.get(AstyanaxIO.getColumnFamilyMapper().get(Granularity.FULL.name()))
                                    .fromByteBuffer(byteBuffer),
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
            ColumnFamily<Locator, Long> CF_metrics_full = AstyanaxIO.getColumnFamilyMapper().get(Granularity.FULL.name());
            ByteBuffer serialized = NumericSerializer.get(CF_metrics_full).toByteBuffer(o);
            Assert.assertEquals(o, NumericSerializer.get(CF_metrics_full).fromByteBuffer(serialized));
        }
    }

    @Test
    public void testRollupSerializationAndDeserialization() throws IOException {
        // works the same way as testFullResSerializationAndDeserialization
        
        if (System.getProperty("GENERATE_ROLLUP_SERIALIZATION") != null) {
            OutputStream os = new FileOutputStream("src/test/resources/serializations/rollup_version_" + Constants.VERSION_1_ROLLUP + ".bin", false);
            for (BasicRollup basicRollup : TO_SERIALIZE_BASIC_ROLLUP) {
                for (Granularity g : Granularity.rollupGranularities()) {
                    ColumnFamily<Locator, Long> CF = AstyanaxIO.getColumnFamilyMapper().get(g.name());
                    ByteBuffer bb = NumericSerializer.get(CF).toByteBuffer(basicRollup);
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
            for (int i = 0; i < TO_SERIALIZE_BASIC_ROLLUP.length; i++) {
                for (Granularity g : Granularity.rollupGranularities()) {
                    ByteBuffer bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
                    BasicRollup basicRollup = (BasicRollup) NumericSerializer.get(AstyanaxIO.getColumnFamilyMapper()
                            .get(g.name())).fromByteBuffer(bb);
                    Assert.assertTrue(String.format("Deserialization for rollup broken at %d", version),
                            TO_SERIALIZE_BASIC_ROLLUP[i].equals(basicRollup));
                }
                version += 1;
            }
        }
        
        // current round tripping.
        for (BasicRollup basicRollup : TO_SERIALIZE_BASIC_ROLLUP) {
            for (Granularity g : Granularity.rollupGranularities()) {
                ColumnFamily<Locator, Long> CF = AstyanaxIO.getColumnFamilyMapper().get(g.name());
                ByteBuffer bb = NumericSerializer.get(CF).toByteBuffer(basicRollup);
                Assert.assertTrue(basicRollup.equals(NumericSerializer.get(CF).fromByteBuffer(bb)));
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
        
        for (Granularity gran : Granularity.granularities()) {
            for (int i = 0; i < inputs.length; i++) {
                try {
                    ColumnFamily<Locator, Long> CF = AstyanaxIO.getColumnFamilyMapper().get(gran.name());
                    Object dst = NumericSerializer.get(CF).fromByteBuffer(NumericSerializer.get(CF).toByteBuffer(inputs[i]));
                    Assert.assertEquals(String.format("busted at %s %d", gran.name(), i), expected[i], dst);
                } catch (ClassCastException ex) {
                    ex.printStackTrace();
                    Assert.fail("Serialization should address class type mismatches as IOExceptions");
                } catch (RuntimeException ex) {
                    Assert.assertTrue(ex.getCause() instanceof SerializationException);
                    if (gran == Granularity.FULL)
                        Assert.assertTrue(inputs[i] instanceof BasicRollup);
                    else
                        Assert.assertFalse(inputs[i] instanceof BasicRollup);
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
            ColumnFamily<Locator, Long> CF_metrics_full = AstyanaxIO.getColumnFamilyMapper().get(Granularity.FULL.name());
            buf = NumericSerializer.get(CF_metrics_full).toByteBuffer(Long.MAX_VALUE).array();
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
            ColumnFamily<Locator, Long> CF_metrics_full = AstyanaxIO.getColumnFamilyMapper().get(Granularity.FULL.name());
            NumericSerializer.get(CF_metrics_full).toByteBuffer("words");
        } catch (RuntimeException e) {
            throw e.getCause();
        }
    }
    
    @Test(expected = UnexpectedStringSerializationException.class)
    public void testDeserializeStringDoesNotFail() throws Throwable {
        // this is what a string looked like previously.
        try {
            ColumnFamily<Locator, Long> CF_metrics_full = AstyanaxIO.getColumnFamilyMapper().get(Granularity.FULL.name());
            String serialized = "AHMWVGhpcyBpcyBhIHRlc3Qgc3RyaW5nLg==";
            ByteBuffer bb = ByteBuffer.wrap(Base64.decodeBase64(serialized.getBytes()));
            NumericSerializer.get(CF_metrics_full).fromByteBuffer(bb);
        } catch (RuntimeException ex) {
            throw ex.getCause();
        }
    }
    
    // this was allowed for a brief while. it would represent a regression now.
    @Test(expected = SerializationException.class)
    public void testCannotRoundtripStringWithNullGran() throws Throwable {
        try {
            String expected = "this is a string";
            ColumnFamily<Locator, Long> CF = null;
            ByteBuffer bb = NumericSerializer.get(CF).toByteBuffer(expected);
            String actual = (String)NumericSerializer.get(CF).fromByteBuffer(bb);
            Assert.assertEquals(expected, actual);
        } catch (RuntimeException ex) {
            throw ex.getCause();
        }
    }
    
    @Test(expected = SerializationException.class)
    public void testCannotRoundtripBytesWillNullGran() throws Throwable {
        try {
            byte[] expected = new byte[] {1,2,3,4,5};
            ColumnFamily<Locator, Long> CF = null;
            ByteBuffer bb = NumericSerializer.get(CF).toByteBuffer(expected);
            byte[] actual = (byte[])NumericSerializer.get(CF).fromByteBuffer(bb);
            Assert.assertArrayEquals(expected, actual);
        } catch (RuntimeException ex) {
            throw ex.getCause();
        }
    }
    
    @Test(expected = SerializationException.class)
    public void testCannotRoundtripBytes() throws Throwable {
        try {
            byte[] expected = new byte[] {1,2,3,4,5};
            ColumnFamily<Locator, Long> CF_metrics_full = AstyanaxIO.getColumnFamilyMapper().get(Granularity.FULL.name());
            AbstractSerializer ser = NumericSerializer.get(CF_metrics_full);
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
        Points<BasicRollup> rollupGroup = new Points<BasicRollup>();
        BasicRollup startingRollup = new BasicRollup();
        startingRollup.setCount(500);
        rollupGroup.add(new Points.Point<BasicRollup>(123456789L, startingRollup));
        
        for (int rollupCount = 0; rollupCount < 500; rollupCount++) {
            Points<SimpleNumber> input = new Points<SimpleNumber>();
            for (int fullResCount = 0; fullResCount < 500; fullResCount++) {
                input.add(new Points.Point<SimpleNumber>(123456789L + fullResCount, new SimpleNumber(fullResCount + fullResCount * 3)));
            }
            BasicRollup basicRollup = BasicRollup.buildRollupFromRawSamples(input);
            Points<BasicRollup> rollups = new Points<BasicRollup>();
            rollups.add(new Points.Point<BasicRollup>(123456789L , basicRollup));
            BasicRollup groupRollup = BasicRollup.buildRollupFromRollups(rollups);
            rollupGroup.add(new Points.Point<BasicRollup>(123456789L, groupRollup));
        }
        
        ColumnFamily<Locator, Long> CF_metrics_240 = AstyanaxIO.getColumnFamilyMapper().get(Granularity.MIN_240.name());
        
        BasicRollup r = BasicRollup.buildRollupFromRollups(rollupGroup);

        // serialization was broken.
        ByteBuffer bb = NumericSerializer.get(CF_metrics_240).toByteBuffer(r);
        Assert.assertEquals(r, NumericSerializer.get(CF_metrics_240).fromByteBuffer(bb));
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