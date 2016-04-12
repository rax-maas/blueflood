/*
 * Copyright 2016 Rackspace
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

import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.exceptions.UnexpectedStringSerializationException;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.io.serializers.Serializers;
import com.rackspacecloud.blueflood.types.SimpleNumber;
import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;

public class StringSerializerTest {

    private String TEST_STRING = "This is a test string.";

    @Test(expected = SerializationException.class)
    public void testSerializerForStringShouldFail() throws Throwable {
        try {
            Serializers.serializerFor(String.class);
        } catch (RuntimeException re) {
            throw re.getCause();
        }
    }

    // we used to allow de-serializing strings, but we don't anymore.
    // catch that error and assert it happens only when expected.
    @Test (expected = UnexpectedStringSerializationException.class)
    public void testStringFullSerializationShouldFail() throws Throwable {
        ByteArrayOutputStream baos = serializeString(TEST_STRING);
        ByteBuffer byteBuffer = ByteBuffer.wrap(baos.toByteArray());
        try {
            Serializers.serializerFor(Object.class).fromByteBuffer(byteBuffer);
        } catch (RuntimeException re) {
            throw re.getCause();
        }
    }

    @Test(expected = UnexpectedStringSerializationException.class)
    public void testDeserializeStringDoesNotFail() throws Throwable {
        // this is what a string looked like previously.
        try {
            String serialized = "AHMWVGhpcyBpcyBhIHRlc3Qgc3RyaW5nLg==";
            ByteBuffer bb = ByteBuffer.wrap(Base64.decodeBase64(serialized.getBytes()));
            Serializers.serializerFor(SimpleNumber.class).fromByteBuffer(bb);
        } catch (RuntimeException ex) {
            throw ex.getCause();
        }
    }

    // this was allowed for a brief while. it would represent a regression now.
    @Test(expected = SerializationException.class)
    public void testCannotRoundtripStringWithNullType() throws Throwable {
        try {
            String expected = "this is a string";
            ByteBuffer bb = Serializers.serializerFor((Class) null).toByteBuffer(expected);
            String actual = (String) Serializers.serializerFor((Class) null).fromByteBuffer(bb);
            Assert.assertEquals(expected, actual);
        } catch (RuntimeException ex) {
            throw ex.getCause();
        }
    }

    // At some point of time, we had some serializer for String class.
    // We serialized one string metric and saved it to the file
    // src/test/resources/serializers/full_version_0.bin.
    // This method was constructed after analyzing the content
    // of that file. The actual code serializing the string in
    // the main code is no longer there.
    private ByteArrayOutputStream serializeString(String input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write(Constants.VERSION_1_FULL_RES);  // version
        baos.write(Constants.STR);                 // type
        baos.write(input.length());                // string length
        baos.write(input.getBytes());
        return baos;
    }
}
