package com.rackspacecloud.blueflood.types;

import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.io.NumericSerializer;
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

public class SetRollupTest {
    
    @Test
    public void testMaths() throws IOException {
        // sum = 60, avg = 20, min = 10, max = 30, count = 3
        Points<SimpleNumber> set0 = new Points<SimpleNumber>() {{
            add(new Point<SimpleNumber>(0, new SimpleNumber(10L)));
            add(new Point<SimpleNumber>(1, new SimpleNumber(20L)));
            add(new Point<SimpleNumber>(2, new SimpleNumber(30L)));
        }};  
        final SetRollup rollup0 = SetRollup.buildRollupFromRawSamples(set0);
        
        Assert.assertEquals(3, rollup0.getCount());
        Assert.assertEquals(20L, rollup0.getAverage().toLong());
        Assert.assertEquals(10L, rollup0.getMinValue().toLong());
        Assert.assertEquals(30L, rollup0.getMaxValue().toLong());
        
        // sum = 220, avg = 55, min = 40, max = 50, count = 4
        Points<SimpleNumber> set1 = new Points<SimpleNumber>() {{
            add(new Point<SimpleNumber>(3, new SimpleNumber(40L)));
            add(new Point<SimpleNumber>(4, new SimpleNumber(50L)));
            add(new Point<SimpleNumber>(5, new SimpleNumber(60L)));
            add(new Point<SimpleNumber>(6, new SimpleNumber(70L)));
        }};
        final SetRollup rollup1 = SetRollup.buildRollupFromRawSamples(set1);
        
        Assert.assertEquals(4, rollup1.getCount());
        Assert.assertEquals(55L, rollup1.getAverage().toLong());
        Assert.assertEquals(40L, rollup1.getMinValue().toLong());
        Assert.assertEquals(70L, rollup1.getMaxValue().toLong());
        
        // now combine them!
        SetRollup rollup2 = SetRollup.buildRollupFromSetRollups(new Points<SetRollup>() {{
            add(new Point<SetRollup>(0, rollup0));
            add(new Point<SetRollup>(3, rollup1));
        }});
        
        long expectedAverage = (rollup0.getAverage().toLong() * rollup0.getCount() + rollup1.getAverage().toLong() * rollup1.getCount()) / (rollup0.getCount() + rollup1.getCount());
        Assert.assertEquals(rollup0.getCount() + rollup1.getCount(), rollup2.getCount());
        Assert.assertEquals(expectedAverage, rollup2.getAverage().toLong());
        Assert.assertEquals(10L, rollup2.getMinValue().toLong());
        Assert.assertEquals(70L, rollup2.getMaxValue().toLong());
    }
    
    
    @Test
    public void testSetV1RoundTrip() throws IOException {
        SetRollup s0 = new SetRollup();
        s0.setCount(32323523);
        s0.setAverage(1);
        s0.setMin(2);
        s0.setMax(3);
        s0.setVariance(2.1d);
        SetRollup s1 = new SetRollup();
        s1.setCount(84234);
        s1.setAverage(4);
        s1.setMin(5);
        s1.setMax(6);
        s1.setVariance(4.522d);
        
        if (System.getProperty("GENERATE_SET_SERIALIZATION") != null) {
            OutputStream os = new FileOutputStream("src/test/resources/serializations/set_version_" + Constants.VERSION_1_SINGLE_VALUE_ROLLUP + ".bin", false);
            os.write(Base64.encodeBase64(new NumericSerializer.SetRollupSerializer().toByteBuffer(s0).array()));
            os.write("\n".getBytes());
            os.write(Base64.encodeBase64(new NumericSerializer.SetRollupSerializer().toByteBuffer(s1).array()));
            os.write("\n".getBytes());
            os.close();
        }
        
        Assert.assertTrue(new File("src/test/resources/serializations").exists());
                
        int count = 0;
        int version = 0;
        final int maxVersion = Constants.VERSION_1_SINGLE_VALUE_ROLLUP;
        while (version <= maxVersion) {
            BufferedReader reader = new BufferedReader(new FileReader("src/test/resources/serializations/set_version_" + version + ".bin"));
            
            ByteBuffer bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
            SetRollup ss0 = NumericSerializer.serializerFor(SetRollup.class).fromByteBuffer(bb);
            Assert.assertEquals(s0, ss0);
            
            bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
            SetRollup ss1 = NumericSerializer.serializerFor(SetRollup.class).fromByteBuffer(bb);
            Assert.assertEquals(s1, ss1);
            
            Assert.assertFalse(ss0.equals(ss1));
            version++;
            count++;
        }
        
        Assert.assertTrue(count > 0);
    }
}
