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

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.netflix.astyanax.serializers.AbstractSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;

public class StringMetadataSerializer extends AbstractSerializer<String> {
    private static final StringMetadataSerializer INSTANCE = new StringMetadataSerializer();
    private static final byte STRING = 4; // History: this used to support multiple types.
    // However, we never actually used that functionality, and it duplicated a lot of logic
    // as is contained in NumericSerializer, without actually being in a compatible [de]serialization format.
    // TODO: re-add non-str functionality in a way that does not involve as much code duplication and format incompatibility

    public static StringMetadataSerializer get() {
        return INSTANCE;
    }

    @Override
    public ByteBuffer toByteBuffer(String o) {
        try {
            byte[] buf = new byte[computeBufLength(o)];
            CodedOutputStream out = CodedOutputStream.newInstance(buf);
            writeToOutputStream(o, out);
            return ByteBuffer.wrap(buf);
        } catch (IOException e) {
            throw new RuntimeException("Serialization problems", e);
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

    @Override
    public String fromByteBuffer(ByteBuffer byteBuffer) {
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
}
