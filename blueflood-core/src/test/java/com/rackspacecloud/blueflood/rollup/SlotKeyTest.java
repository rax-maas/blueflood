package com.rackspacecloud.blueflood.rollup;

import org.junit.Assert;
import org.junit.Test;

public class SlotKeyTest {
    @Test
    public void test_parse() {
        SlotKey parsed = SlotKey.parse("metrics_1440m,10,20");
        Assert.assertEquals(parsed.getGranularity(), Granularity.MIN_1440);
        Assert.assertEquals(parsed.getSlot(), 10);
        Assert.assertEquals(parsed.getShard(), 20);

        // invalid results
        Assert.assertNull(SlotKey.parse("metrics_6m,10,20"));
        Assert.assertNull(SlotKey.parse("metrics_1440m,200,20"));
        Assert.assertNull(SlotKey.parse("metrics_1440m,10,128"));
    }

    @Test
    public void test_getChildrenKeys() {
        // the relationship between slots plays out here.
        int shard = 0;
        int slot = 0;

        // full res slots have no children.
        int expect = 0;
        Assert.assertEquals(expect, SlotKey.of(Granularity.FULL, slot, shard).getChildrenKeys().size());

        // min5 slots contain only their full res counterpart.
        expect = expect * 1 + 1;
        Assert.assertEquals(expect, SlotKey.of(Granularity.MIN_5, slot, shard).getChildrenKeys().size());

        // min20 slots contain their 4 min5 children and full res grandchildren.
        expect = expect * 4 + 4;
        Assert.assertEquals(expect, SlotKey.of(Granularity.MIN_20, slot, shard).getChildrenKeys().size());

        // and so on. the relationship between min60 and min20 is a factor of 3.
        expect = expect * 3 + 3;
        Assert.assertEquals(expect, SlotKey.of(Granularity.MIN_60, slot, shard).getChildrenKeys().size());

        // min240 and min60 is a factor of 4.
        expect = expect * 4 + 4;
        Assert.assertEquals(expect, SlotKey.of(Granularity.MIN_240, slot, shard).getChildrenKeys().size());

        // min1440 and min240 is a factor of 6.
        expect = expect * 6 + 6;
        Assert.assertEquals(expect, SlotKey.of(Granularity.MIN_1440, slot, shard).getChildrenKeys().size());
    }
}
