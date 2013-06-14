package com.cloudkick.blueflood.io;

import com.cloudkick.blueflood.types.Locator;
import com.google.common.base.Charsets;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class LocatorSerializer extends AbstractSerializer<Locator>{
    private static final LocatorSerializer instance = new LocatorSerializer();
    private static final Charset charset = Charsets.UTF_8;


    public static LocatorSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(Locator locator) {
        return StringSerializer.get().toByteBuffer(locator.toString());
    }

    @Override
    public Locator fromByteBuffer(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return null;
        }
        return new Locator(charset.decode(byteBuffer).toString());
    }
}
