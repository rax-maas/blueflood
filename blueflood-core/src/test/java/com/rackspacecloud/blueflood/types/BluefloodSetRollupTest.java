package com.rackspacecloud.blueflood.types;

import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.io.serializers.NumericSerializer;
import junit.framework.Assert;
import org.apache.commons.codec.binary.Base64;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class BluefloodSetRollupTest {
    
    @Test
    public void testMaths() throws IOException {
        // sum = 60, avg = 20, min = 10, max = 30, count = 3
        Points<SimpleNumber> set0 = new Points<SimpleNumber>() {{
            add(new Point<SimpleNumber>(0, new SimpleNumber(10L)));
            add(new Point<SimpleNumber>(1, new SimpleNumber(20L)));
            add(new Point<SimpleNumber>(2, new SimpleNumber(30L)));
        }};  
        final BluefloodSetRollup rollup0 = new BluefloodSetRollup()
                .withObject(10)
                .withObject(20)
                .withObject(30);
        
        Assert.assertEquals(3, rollup0.getCount());
        
        Points<SimpleNumber> set1 = new Points<SimpleNumber>() {{
            add(new Point<SimpleNumber>(3, new SimpleNumber(40L)));
            add(new Point<SimpleNumber>(4, new SimpleNumber(50L)));
            add(new Point<SimpleNumber>(5, new SimpleNumber(60L)));
            add(new Point<SimpleNumber>(6, new SimpleNumber(70L)));
        }};
        final BluefloodSetRollup rollup1 = new BluefloodSetRollup()
                .withObject(40)
                .withObject(50)
                .withObject(60)
                // notice the duplicates being sent in here. they should be ignored.
                .withObject(70)
                .withObject(70)
                .withObject(70);
        
        Assert.assertEquals(4, rollup1.getCount());
        
        // now combine them!
        BluefloodSetRollup rollup2 = BluefloodSetRollup.buildRollupFromSetRollups(new Points<BluefloodSetRollup>() {{
            add(new Point<BluefloodSetRollup>(0, rollup0));
            add(new Point<BluefloodSetRollup>(3, rollup1));
        }});
        
        Assert.assertEquals(4 + 3, rollup2.getCount());
    }
    
    
    @Test
    public void testSetV1RoundTrip() throws IOException {
        final Random rand = new Random(7391938383L);
        Set<String> s0Sample = new HashSet<String>();
        Set<String> s1Sample = new HashSet<String>();
        BluefloodSetRollup s0 = new BluefloodSetRollup();
        BluefloodSetRollup s1 = new BluefloodSetRollup();
        
        for (int i = 0; i < 200; i++) {
            String s0String = Long.toHexString(rand.nextLong());
            String s1String = Long.toHexString(rand.nextLong());
            s0 = s0.withObject(s0String);
            s1 = s1.withObject(s1String);
            if (i % 5 == 0) {
                s0Sample.add(s0String);
                s1Sample.add(s1String);
            }
        }
        
        if (System.getProperty("GENERATE_SET_SERIALIZATION") != null) {
            OutputStream os = new FileOutputStream("src/test/resources/serializations/set_version_" + Constants.VERSION_1_SET_ROLLUP + ".bin", false);
            os.write(Base64.encodeBase64(new NumericSerializer.SetRollupSerializer().toByteBuffer(s0).array()));
            os.write("\n".getBytes());
            os.write(Base64.encodeBase64(new NumericSerializer.SetRollupSerializer().toByteBuffer(s1).array()));
            os.write("\n".getBytes());
            os.close();
        }
        
        Assert.assertTrue(new File("src/test/resources/serializations").exists());
                
        int count = 0;
        int version = 0;
        final int maxVersion = Constants.VERSION_1_SET_ROLLUP;
        while (version <= maxVersion) {
            BufferedReader reader = new BufferedReader(new FileReader("src/test/resources/serializations/set_version_" + version + ".bin"));
            
            ByteBuffer bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
            BluefloodSetRollup ss0 = NumericSerializer.serializerFor(BluefloodSetRollup.class).fromByteBuffer(bb);
            Assert.assertEquals(s0, ss0);
            
            bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
            BluefloodSetRollup ss1 = NumericSerializer.serializerFor(BluefloodSetRollup.class).fromByteBuffer(bb);
            Assert.assertEquals(s1, ss1);
            
            Assert.assertFalse(ss0.equals(ss1));
            
            for (String s0String : s0Sample) {
                Assert.assertTrue(ss0.contains(s0String));
                Assert.assertTrue(s0.contains(s0String));
                Assert.assertFalse(ss1.contains(s0String));
                Assert.assertFalse(ss1.contains(s0String));
            }
            for (String s1String : s1Sample) {
                Assert.assertFalse(ss0.contains(s1String));
                Assert.assertFalse(s0.contains(s1String));
                Assert.assertTrue(ss1.contains(s1String));
                Assert.assertTrue(s1.contains(s1String));
            }
            
            version++;
            count++;
        }
        
        Assert.assertTrue(count > 0);
    }
}
