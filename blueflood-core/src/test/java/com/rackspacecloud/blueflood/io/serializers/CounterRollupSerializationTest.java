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

import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.types.BluefloodCounterRollup;
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

public class CounterRollupSerializationTest {

    @Test
    public void testCounterV1RoundTrip() throws IOException {
        BluefloodCounterRollup c0 = new BluefloodCounterRollup().withCount(7442245).withSampleCount(1);
        BluefloodCounterRollup c1 = new BluefloodCounterRollup().withCount(34454722343L).withSampleCount(10);
        
        if (System.getProperty("GENERATE_COUNTER_SERIALIZATION") != null) {
            OutputStream os = new FileOutputStream("src/test/resources/serializations/counter_version_" + Constants.VERSION_1_COUNTER_ROLLUP + ".bin", false);
            os.write(Base64.encodeBase64(new NumericSerializer.CounterRollupSerializer().toByteBuffer(c0).array()));
            os.write("\n".getBytes());
            os.write(Base64.encodeBase64(new NumericSerializer.CounterRollupSerializer().toByteBuffer(c1).array()));
            os.write("\n".getBytes());
            os.close();
        }
        
        Assert.assertTrue(new File("src/test/resources/serializations").exists());
                
        int count = 0;
        int version = 0;
        final int maxVersion = Constants.VERSION_1_COUNTER_ROLLUP;
        while (version <= maxVersion) {
            BufferedReader reader = new BufferedReader(new FileReader("src/test/resources/serializations/counter_version_" + version + ".bin"));
            
            ByteBuffer bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
            BluefloodCounterRollup cc0 = NumericSerializer.serializerFor(BluefloodCounterRollup.class).fromByteBuffer(bb);
            Assert.assertEquals(c0, cc0);
            
            bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
            BluefloodCounterRollup cc1 = NumericSerializer.serializerFor(BluefloodCounterRollup.class).fromByteBuffer(bb);
            Assert.assertEquals(c1, cc1);
            
            Assert.assertFalse(cc0.equals(cc1));
            version++;
            count++;
        }
        
        Assert.assertTrue(count > 0);
    }
}
