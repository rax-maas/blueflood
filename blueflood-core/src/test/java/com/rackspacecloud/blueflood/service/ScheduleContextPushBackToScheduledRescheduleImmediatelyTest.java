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


    long now;
    long fiveMinutes;
    long updateTime1;
    long updateTime2;
    Granularity gran;
    int slot1;
    int slot2;
    ScheduleContext ctx;
    SlotKey next;
    int nextSlot;
    int otherSlot;

    @Before
    public void setUp() {

        now = 1234000L;
        fiveMinutes = 5 * 60 * 1000;
        updateTime1 = now - 2;
        updateTime2 = now - fiveMinutes - 2;
        shard = shards.get(0);
        gran = Granularity.MIN_5;
        slot1 = gran.slot(now);
        slot2 = gran.slot(now - fiveMinutes);

        ctx = new ScheduleContext(now, shards);
        ctx.update(updateTime1, shard);
        ctx.update(updateTime2, shard);
        ctx.scheduleEligibleSlots(1, 7200000, 3600000);

        // precondition
        Assert.assertEquals(2, ctx.getScheduledCount());

        next = ctx.getNextScheduled();
        Assert.assertEquals(shard, next.getShard());
        Assert.assertEquals(gran, next.getGranularity());
        nextSlot = next.getSlot();
        otherSlot = (nextSlot == slot1 ? slot2 : slot1);

        Assert.assertEquals(1, ctx.getScheduledCount());
    }

    @Test
    public void testPushBackToScheduledRescheduleImmediately() {

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
