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
package com.rackspacecloud.blueflood.io.serializers.metrics;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This class knows how to serialize/deserialize StringMetadata objects.
 */
public class StringMetadataSerDes {

    private static final byte STRING = 4; // History: this used to support multiple types.

    public ByteBuffer serialize(String str) {
        try {
            byte[] buf = new byte[computeBufLength(str)];
            CodedOutputStream out = CodedOutputStream.newInstance(buf);
            writeToOutputStream(str, out);
            return ByteBuffer.wrap(buf);
        } catch (IOException e) {
            throw new RuntimeException("Error serializing string metadata", e);
        }
    }

    public String deserialize(ByteBuffer byteBuffer) {
        CodedInputStream is = CodedInputStream.newInstance(byteBuffer.array());
        try {
            byte type = is.readRawByte();
            if (type == STRING) {
                return is.readString();
            } else {
                throw new IOException("Unexpected first byte. Expected '4' (string). Got '" + type + "'.");
            }
        } catch (IOException e) {
            throw new RuntimeException("IOException during deserialization", e);
        }
    }

    // writes object to CodedOutputStream.
    private static void writeToOutputStream(Object obj, CodedOutputStream out) throws IOException {
        out.writeRawByte(STRING);
        out.writeStringNoTag((String)obj);
    }

    // figure out how much space it will take to encode an object. this makes it so that we only allocate one buffer
    // during encode.
    private static int computeBufLength(String obj) throws IOException {
        return 1 + CodedOutputStream.computeStringSizeNoTag(obj); // 1 for type
    }
}
