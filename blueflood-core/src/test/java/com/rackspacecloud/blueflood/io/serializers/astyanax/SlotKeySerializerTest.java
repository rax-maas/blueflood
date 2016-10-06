package com.rackspacecloud.blueflood.io.serializers.astyanax;

import com.netflix.astyanax.serializers.StringSerializer;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class SlotKeySerializerTest {

    @Test
    public void testToFromByteBuffer() {
        Granularity expectedGranularity = Granularity.MIN_5;
        int expectedSlot = 10;
        int expectedShard = 1;

        ByteBuffer origBuff = StringSerializer.get().toByteBuffer(
                SlotKey.of(expectedGranularity, expectedSlot, expectedShard).toString());
        Assert.assertNotNull(origBuff);

        SlotKey slotKey = SlotKeySerializer.get().fromByteBuffer(origBuff.duplicate());
        Assert.assertEquals("Invalid granularity", expectedGranularity, slotKey.getGranularity());
        Assert.assertEquals("Invalid slot", expectedSlot, slotKey.getSlot());
        Assert.assertEquals("Invalid shard", expectedShard, slotKey.getShard());

        ByteBuffer newBuff = SlotKeySerializer.get().toByteBuffer(slotKey);
        Assert.assertEquals(origBuff, newBuff);
    }
}
