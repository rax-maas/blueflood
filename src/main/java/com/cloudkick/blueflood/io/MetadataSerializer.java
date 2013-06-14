package com.cloudkick.blueflood.io;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.netflix.astyanax.serializers.AbstractSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;

public class MetadataSerializer extends AbstractSerializer<Object> {
    private static final MetadataSerializer INSTANCE = new MetadataSerializer();
    private static final byte STRING = 4; // History: this used to support multiple types.
    // However, we never actually used that functionality, and it duplicated a lot of logic
    // as is contained in NumericSerializer, without actually being in a compatible [de]serialization format.
    // TODO: re-add non-str functionality in a way that does not involve as much code duplication and format incompatibility

    public static MetadataSerializer get() {
        return INSTANCE;
    }

    @Override
    public ByteBuffer toByteBuffer(Object o) {
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
    private static int computeBufLength(Object obj) throws IOException {
        return 1 + CodedOutputStream.computeStringSizeNoTag((String)obj); // 1 for type
    }

    @Override
    public Object fromByteBuffer(ByteBuffer byteBuffer) {
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
