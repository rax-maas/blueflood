package com.rackspacecloud.blueflood.io.serializers.astyanax;

import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.rackspacecloud.blueflood.io.serializers.metrics.SlotKeySerDes;
import com.rackspacecloud.blueflood.rollup.SlotKey;

import java.nio.ByteBuffer;

public class SlotKeySerializer extends AbstractSerializer<SlotKey> {

    private static final SlotKeySerDes serDes = new SlotKeySerDes();
    private static final SlotKeySerializer INSTANCE = new SlotKeySerializer();

    public static SlotKeySerializer get() {
        return INSTANCE;
    }

    @Override
    public ByteBuffer toByteBuffer(SlotKey slotKey) {
        return StringSerializer.get().toByteBuffer(serDes.serialize(slotKey));
    }

    @Override
    public SlotKey fromByteBuffer(ByteBuffer byteBuffer) {
        String stringRep = StringSerializer.get().fromByteBuffer(byteBuffer);
        return serDes.deserialize(stringRep);
    }
}
