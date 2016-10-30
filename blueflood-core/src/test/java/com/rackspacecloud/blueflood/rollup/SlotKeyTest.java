package com.rackspacecloud.blueflood.rollup;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SlotKeyTest {
    @Test
    public void test_parse() {
        SlotKey parsed = SlotKey.parse("metrics_1440m,10,20");
        assertEquals(parsed.getGranularity(), Granularity.MIN_1440);
        assertEquals(parsed.getSlot(), 10);
        assertEquals(parsed.getShard(), 20);

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
        assertEquals(expect, SlotKey.of(Granularity.FULL, slot, shard).getChildrenKeys().size());

        // min5 slots contain only their full res counterpart.
        expect = expect * 1 + 1;
        assertEquals(expect, SlotKey.of(Granularity.MIN_5, slot, shard).getChildrenKeys().size());

        // min20 slots contain their 4 min5 children and full res grandchildren.
        expect = expect * 4 + 4;
        assertEquals(expect, SlotKey.of(Granularity.MIN_20, slot, shard).getChildrenKeys().size());

        // and so on. the relationship between min60 and min20 is a factor of 3.
        expect = expect * 3 + 3;
        assertEquals(expect, SlotKey.of(Granularity.MIN_60, slot, shard).getChildrenKeys().size());

        // min240 and min60 is a factor of 4.
        expect = expect * 4 + 4;
        assertEquals(expect, SlotKey.of(Granularity.MIN_240, slot, shard).getChildrenKeys().size());

        // min1440 and min240 is a factor of 6.
        expect = expect * 6 + 6;
        assertEquals(expect, SlotKey.of(Granularity.MIN_1440, slot, shard).getChildrenKeys().size());
    }

    @Test
    public void testExtrapolate() {
        int shard = 1;

        //full -> 5m
        assertEquals("Invalid extrapolation - scenario 0", SlotKey.of(Granularity.MIN_5, 1, shard),
                SlotKey.of(Granularity.FULL, 1, shard).extrapolate(Granularity.MIN_5));

        assertEquals("Invalid extrapolation - scenario 1", SlotKey.of(Granularity.MIN_5, 43, shard),
                SlotKey.of(Granularity.FULL, 43, shard).extrapolate(Granularity.MIN_5));

        //5m -> 5m
        assertEquals("Invalid extrapolation - scenario 2", SlotKey.of(Granularity.MIN_5, 24, shard),
                SlotKey.of(Granularity.MIN_5, 24, shard).extrapolate(Granularity.MIN_5));

        //5m -> 20m
        assertEquals("Invalid extrapolation - scenario 3", SlotKey.of(Granularity.MIN_20, 0, shard),
                SlotKey.of(Granularity.MIN_5, 0, shard).extrapolate(Granularity.MIN_20));

        assertEquals("Invalid extrapolation - scenario 4", SlotKey.of(Granularity.MIN_20, 0, shard),
                SlotKey.of(Granularity.MIN_5, 3, shard).extrapolate(Granularity.MIN_20));

        assertEquals("Invalid extrapolation - scenario 5", SlotKey.of(Granularity.MIN_20, 1, shard),
                SlotKey.of(Granularity.MIN_5, 4, shard).extrapolate(Granularity.MIN_20));

        assertEquals("Invalid extrapolation - scenario 6", SlotKey.of(Granularity.MIN_20, 4032 / 4 - 1, shard),
                SlotKey.of(Granularity.MIN_5, 4031, shard).extrapolate(Granularity.MIN_20));

        //5m -> 60m
        assertEquals("Invalid extrapolation - scenario 7", SlotKey.of(Granularity.MIN_60, 0, shard),
                SlotKey.of(Granularity.MIN_5, 3, shard).extrapolate(Granularity.MIN_60));

        assertEquals("Invalid extrapolation - scenario 8", SlotKey.of(Granularity.MIN_60, 0, shard),
                SlotKey.of(Granularity.MIN_5, 3, shard).extrapolate(Granularity.MIN_60));

        assertEquals("Invalid extrapolation - scenario 9", SlotKey.of(Granularity.MIN_60, 1, shard),
                SlotKey.of(Granularity.MIN_5, 12, shard).extrapolate(Granularity.MIN_60));

        assertEquals("Invalid extrapolation - scenario 10", SlotKey.of(Granularity.MIN_60, 4032 / 12 - 1, shard),
                SlotKey.of(Granularity.MIN_5, 4031, shard).extrapolate(Granularity.MIN_60));

        //5m -> 1440m
        assertEquals("Invalid extrapolation - scenario 11", SlotKey.of(Granularity.MIN_1440, 0, shard),
                SlotKey.of(Granularity.MIN_5, 3, shard).extrapolate(Granularity.MIN_1440));

        assertEquals("Invalid extrapolation - scenario 12", SlotKey.of(Granularity.MIN_1440, 0, shard),
                SlotKey.of(Granularity.MIN_5, 287, shard).extrapolate(Granularity.MIN_1440));

        assertEquals("Invalid extrapolation - scenario 13", SlotKey.of(Granularity.MIN_1440, 1, shard),
                SlotKey.of(Granularity.MIN_5, 288, shard).extrapolate(Granularity.MIN_1440));

        //20m -> 60m
        assertEquals("Invalid extrapolation - scenario 14", SlotKey.of(Granularity.MIN_60, 0, shard),
                SlotKey.of(Granularity.MIN_20, 1, shard).extrapolate(Granularity.MIN_60));

        assertEquals("Invalid extrapolation - scenario 15", SlotKey.of(Granularity.MIN_60, 0, shard),
                SlotKey.of(Granularity.MIN_20, 2, shard).extrapolate(Granularity.MIN_60));

        assertEquals("Invalid extrapolation - scenario 16", SlotKey.of(Granularity.MIN_60, 1, shard),
                SlotKey.of(Granularity.MIN_20, 3, shard).extrapolate(Granularity.MIN_60));

    }

    @Test(expected = IllegalArgumentException.class)
    public void testExtrapolateWithLowerDestGranularity1() {
        SlotKey.of(Granularity.MIN_20, 1, 1).extrapolate(Granularity.MIN_5);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExtrapolateWithLowerDestGranularity2() {
        SlotKey.of(Granularity.MIN_1440, 1, 1).extrapolate(Granularity.MIN_5);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExtrapolateWithLowerDestGranularity3() {
        SlotKey.of(Granularity.MIN_60, 1, 1).extrapolate(Granularity.MIN_20);
    }
}
