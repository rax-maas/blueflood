package com.cloudkick.blueflood.io;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class MetadataSerializerTest {

    // TODO: uncomment most of these, pending https://issues.rax.io/browse/CMD-139
    
//    @Test
//    public void testLong() throws IOException {
//        Object[] values = {
//            Long.MIN_VALUE,
//            Long.MIN_VALUE / 2,
//            -1000L,
//            -1L,
//            0L,
//            1L,
//            1000L,
//            Long.MAX_VALUE / 2,
//            Long.MAX_VALUE
//        };
//        testRoundTrip(values);
//    }
//
//    @Test
//    public void testInteger() throws IOException {
//        Object[] values = {
//            Integer.MIN_VALUE,
//            Integer.MIN_VALUE / 2,
//            -1000,
//            -1,
//            0,
//            1,
//            1000,
//            Integer.MAX_VALUE / 2,
//            Integer.MAX_VALUE
//        };
//        testRoundTrip(values);
//    }
//
//    @Test
//    public void testFloat() throws IOException {
//        Object[] values = {
//            Float.MIN_VALUE,
//            Float.MIN_VALUE / 2,
//            -1000.0f,
//            -1f,
//            0f,
//            1f,
//            1000f,
//            Float.MAX_VALUE / 2,
//            Float.MAX_VALUE
//        };
//        testRoundTrip(values);
//    }
//
//    @Test
//    public void testDouble() throws IOException {
//        Object[] values = {
//            Double.MIN_VALUE,
//            Double.MIN_VALUE / 2,
//            -1000.0f,
//            -1f,
//            0f,
//            1f,
//            1000f,
//            Double.MAX_VALUE / 2,
//            Double.MAX_VALUE
//        };
//        testRoundTrip(values);
//    }
    
    @Test
    public void testString() throws IOException {
        Object[] values = {
            "abcdefg",
            "\u1234 \u0086 \uabcd \u5432",
            "ĆĐÈ¿ΔΞ€"
        };
        testRoundTrip(values);
    }
    
//    @Test
//    public void testBinary() throws IOException {
//        Random rand = new Random(System.currentTimeMillis());
//        byte[][] values = new byte[6][];
//        for (int i = 0; i < values.length; i++) {
//            int target = 1 * (int)Math.pow(10, i);
//            values[i] = new byte[target];
//            rand.nextBytes(values[i]);
//        }
//        testRoundTrip(values);
//    }
    
    private void testRoundTrip(Object[] objects) throws IOException {
        for (Object obj : objects) {
            byte[] buf = MetadataSerializer.get().toByteBuffer(obj).array();
            ByteBuffer bb = ByteBuffer.wrap(buf);
            if (obj instanceof byte[]) {
                assertArrayEquals((byte[])obj, (byte[]) MetadataSerializer.get().fromByteBuffer(bb));
            } else {
                assertEquals(obj, MetadataSerializer.get().fromByteBuffer(bb));
            }
        }
    }
}
