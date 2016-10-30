package com.rackspacecloud.blueflood.io.serializers.metrics;

import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import org.junit.Assert;
import org.junit.Test;

public class SlotKeySerDesTest {

    private static SlotKeySerDes serDes = new SlotKeySerDes();

    @Test
    public void testGranularityFromSlotKey() {
        Granularity expected = Granularity.MIN_5;
        Granularity myGranularity = SlotKeySerDes.granularityFromSlotKey(SlotKey.of(expected, 1, 1).toString());
        Assert.assertNotNull(myGranularity);
        Assert.assertEquals(myGranularity, expected);

        myGranularity = SlotKeySerDes.granularityFromSlotKey("FULL");
        Assert.assertNull(myGranularity);
    }

    @Test
    public void testSlotFromSlotKey() {
        int expectedSlot = 1;
        int slot = SlotKeySerDes.slotFromSlotKey(SlotKey.of(Granularity.MIN_5, expectedSlot, 10).toString());
        Assert.assertEquals(expectedSlot, slot);
    }

    @Test
    public void testShardFromSlotKey() {
        int expectedShard = 1;
        int shard = SlotKeySerDes.shardFromSlotKey(SlotKey.of(Granularity.MIN_5, 10, expectedShard).toString());
        Assert.assertEquals(expectedShard, shard);
    }


}
