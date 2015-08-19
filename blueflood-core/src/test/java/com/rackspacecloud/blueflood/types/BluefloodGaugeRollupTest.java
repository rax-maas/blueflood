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

public class BluefloodGaugeRollupTest {

    @Test
    public void testGaugeV1RoundTrip() throws IOException {
        
        Points<SimpleNumber> set0 = new Points<SimpleNumber>() {{
            add(new Point<SimpleNumber>(0, new SimpleNumber(10L)));
            add(new Point<SimpleNumber>(1, new SimpleNumber(30L)));
            add(new Point<SimpleNumber>(2, new SimpleNumber(20L)));
        }};
        
        Points<SimpleNumber> set1 = new Points<SimpleNumber>() {{
            add(new Point<SimpleNumber>(3, new SimpleNumber(40L)));
            add(new Point<SimpleNumber>(4, new SimpleNumber(70L)));
            add(new Point<SimpleNumber>(5, new SimpleNumber(60L)));
            add(new Point<SimpleNumber>(6, new SimpleNumber(50L)));
        }};
        
        final BluefloodGaugeRollup g0 = BluefloodGaugeRollup.buildFromRawSamples(set0);
        final BluefloodGaugeRollup g1 = BluefloodGaugeRollup.buildFromRawSamples(set1);
        BluefloodGaugeRollup g2 = BluefloodGaugeRollup.buildFromGaugeRollups(new Points<BluefloodGaugeRollup>() {{
            add(new Point<BluefloodGaugeRollup>(0, g0));
            add(new Point<BluefloodGaugeRollup>(3, g1));
        }});
        
        // equality tests.
        Assert.assertTrue(BluefloodGaugeRollup.buildFromRawSamples(set0).equals(BluefloodGaugeRollup.buildFromRawSamples(set0)));
        Assert.assertFalse(g0.equals(g1));
        
        Assert.assertEquals(20L, g0.getLatestNumericValue());
        Assert.assertEquals(50L, g1.getLatestNumericValue());
        
        Assert.assertEquals(40L, g2.getAverage().toLong());
        Assert.assertEquals(7, g2.getCount());
        Assert.assertEquals(10L, g2.getMinValue().toLong());
        Assert.assertEquals(70L, g2.getMaxValue().toLong());
        Assert.assertEquals(50L, g2.getLatestNumericValue());
        Assert.assertEquals(variance(10,20,30,40,50,60,70), g2.getVariance().toDouble());
        
        
        if (System.getProperty("GENERATE_GAUGE_SERIALIZATION") != null) {
            OutputStream os = new FileOutputStream("src/test/resources/serializations/gauge_version_" + Constants.VERSION_1_ROLLUP + ".bin", false);
            os.write(Base64.encodeBase64(new NumericSerializer.GaugeRollupSerializer().toByteBuffer(g0).array()));
            os.write("\n".getBytes());
            os.write(Base64.encodeBase64(new NumericSerializer.GaugeRollupSerializer().toByteBuffer(g1).array()));
            os.write("\n".getBytes());
            os.close();
        }

        Assert.assertTrue(new File("src/test/resources/serializations").exists());
        
        int count = 0;
        int version = 0;
        final int maxVersion = Constants.VERSION_1_COUNTER_ROLLUP;
        while (version <= maxVersion) {
            BufferedReader reader = new BufferedReader(new FileReader("src/test/resources/serializations/gauge_version_" + version + ".bin"));
            
            ByteBuffer bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
            BluefloodGaugeRollup gg0 = NumericSerializer.serializerFor(BluefloodGaugeRollup.class).fromByteBuffer(bb);
            Assert.assertEquals(g0, gg0);
            
            bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
            BluefloodGaugeRollup gg1 = NumericSerializer.serializerFor(BluefloodGaugeRollup.class).fromByteBuffer(bb);
            Assert.assertEquals(g1, gg1);
            
            Assert.assertFalse(gg0.equals(gg1));
            version++;
            count++;
        }
        
        Assert.assertTrue(count > 0);
    }
    
    // populational variance.
    private static double variance(long... nums) {
        double sum = 0d;
        for (long x : nums)
            sum += x;
        double avg = sum / (double)nums.length;
        double squaredDifferences = 0;
        for (long x : nums)
            squaredDifferences += Math.pow(avg - (double)x, 2d);
        return squaredDifferences / (nums.length);
    }
}
