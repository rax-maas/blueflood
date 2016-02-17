package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ScheduleContextPushBackToScheduledRescheduleImmediatelyTest {

    private static List<Integer> shards = new ArrayList<Integer>() {{ add(shard); }};
    private static int shard = 0;

    @Before
    public void setUp() {
    }

    @Test
    public void testPushBackToScheduledRescheduleImmediately() {

        // given
        long now = 1234000L;
        long fiveMinutes = 5 * 60 * 1000;
        long updateTime1 = now - 2;
        long updateTime2 = now - fiveMinutes - 2;
        int shard = shards.get(0);
        Granularity gran = Granularity.MIN_5;

        ScheduleContext ctx = new ScheduleContext(now, shards);
        ctx.update(updateTime1, shard);
        ctx.update(updateTime2, shard);
        ctx.scheduleSlotsOlderThan(1);

        // precondition
        Assert.assertEquals(2, ctx.getScheduledCount());

        SlotKey next = ctx.getNextScheduled();
        Assert.assertEquals(shard, next.getShard());
        Assert.assertEquals(gran, next.getGranularity());
        int nextSlot = next.getSlot();

        Assert.assertEquals(1, ctx.getScheduledCount());

        // when
        ctx.pushBackToScheduled(next, true);

        // then
        Assert.assertEquals(2, ctx.getScheduledCount());
        next = ctx.getNextScheduled();
        Assert.assertEquals(shard, next.getShard());
        Assert.assertEquals(nextSlot, next.getSlot());
        Assert.assertEquals(gran, next.getGranularity());
        Assert.assertEquals(1, ctx.getScheduledCount());
    }

    @Test
    public void testPushBackToScheduledDontRescheduleImmediately() {

        // given
        long now = 1234000L;
        long fiveMinutes = 5 * 60 * 1000;
        long updateTime1 = now - 2;
        long updateTime2 = now - fiveMinutes - 2;
        int shard = shards.get(0);
        Granularity gran = Granularity.MIN_5;
        int slot1 = gran.slot(now);
        int slot2 = gran.slot(now - fiveMinutes);

        ScheduleContext ctx = new ScheduleContext(now, shards);
        ctx.update(updateTime1, shard);
        ctx.update(updateTime2, shard);
        ctx.scheduleSlotsOlderThan(1);

        // precondition
        Assert.assertEquals(2, ctx.getScheduledCount());

        SlotKey next = ctx.getNextScheduled();
        Assert.assertEquals(shard, next.getShard());
        Assert.assertEquals(gran, next.getGranularity());
        int nextSlot = next.getSlot();
        int otherSlot = (nextSlot == slot1 ? slot2 : slot1);

        Assert.assertEquals(1, ctx.getScheduledCount());

        // when
        ctx.pushBackToScheduled(next, false);

        // then
        Assert.assertEquals(2, ctx.getScheduledCount());
        next = ctx.getNextScheduled();
        Assert.assertEquals(shard, next.getShard());
        Assert.assertEquals(otherSlot, next.getSlot());
        Assert.assertEquals(gran, next.getGranularity());
        Assert.assertEquals(1, ctx.getScheduledCount());
    }
}
