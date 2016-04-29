package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.exceptions.GranularityException;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ScheduleContextPushBackToScheduledTest {

    private static List<Integer> shards = new ArrayList<Integer>() {{ add(shard); }};
    private static int shard = 0;


    long now;
    long updateTime;
    Granularity gran;
    int slot;
    Granularity coarserGran;
    int coarserSlot;

    ScheduleContext ctx;
    ShardStateManager mgr;

    @Before
    public void setUp() throws GranularityException {

        now = 1234000L;
        updateTime = now - 2;
        gran = Granularity.MIN_5;
        slot = gran.slot(now);
        coarserGran = gran.coarser();
        coarserSlot = coarserGran.slot(updateTime);

        ctx = new ScheduleContext(now, shards);
        mgr = ctx.getShardStateManager();
        ctx.update(updateTime, shard);
        ctx.scheduleEligibleSlots(1, 7200000, 3600000);
    }

    @Test
    public void testPushBackToScheduledIncrementsScheduledCount() {

        // given
        SlotKey next = ctx.getNextScheduled();
        Assert.assertEquals(shard, next.getShard());
        Assert.assertEquals(slot, next.getSlot());
        Assert.assertEquals(gran, next.getGranularity());

        Assert.assertEquals(0, ctx.getScheduledCount());

        // when
        ctx.pushBackToScheduled(next, true);

        // then
        Assert.assertEquals(1, ctx.getScheduledCount());
    }

    @Test
    public void testPushBackToScheduled_DOES_NOT_DecrementsRunningCount() {

        // TODO: This functionality is probably wrong. It's related to a fairly
        // rare failure condition so it shouldn't happen often. Nevertheless,
        // we should fix it so that re-scheduling a slot should pull it out of
        // the 'running' category, since it's no longer actually running.

        // given
        SlotKey next = ctx.getNextScheduled();
        Assert.assertEquals(shard, next.getShard());
        Assert.assertEquals(slot, next.getSlot());
        Assert.assertEquals(gran, next.getGranularity());

        Assert.assertEquals(1, ctx.getRunningCount());

        // when
        ctx.pushBackToScheduled(next, true);

        // then
        Assert.assertEquals(1, ctx.getRunningCount());
    }

    @Test
    public void testPushBackToScheduledChangesStateBackToActive() {

        // given
        SlotKey next = ctx.getNextScheduled();
        Assert.assertEquals(shard, next.getShard());
        Assert.assertEquals(slot, next.getSlot());
        Assert.assertEquals(gran, next.getGranularity());

        UpdateStamp stamp = mgr.getUpdateStamp(SlotKey.of(gran, slot, shard));
        Assert.assertNotNull(stamp);
        Assert.assertEquals(UpdateStamp.State.Running, stamp.getState());
        Assert.assertEquals(updateTime, stamp.getTimestamp());
        Assert.assertTrue(stamp.isDirty());

        stamp = mgr.getUpdateStamp(SlotKey.of(coarserGran, coarserSlot, shard));
        Assert.assertNotNull(stamp);
        Assert.assertEquals(UpdateStamp.State.Active, stamp.getState());
        Assert.assertEquals(updateTime, stamp.getTimestamp());
        Assert.assertTrue(stamp.isDirty());

        // when
        ctx.pushBackToScheduled(next, true);

        // then
        stamp = mgr.getUpdateStamp(next);
        Assert.assertNotNull(stamp);
        Assert.assertEquals(UpdateStamp.State.Active, stamp.getState());
        Assert.assertEquals(updateTime, stamp.getTimestamp());
        Assert.assertTrue(stamp.isDirty());

        stamp = mgr.getUpdateStamp(SlotKey.of(coarserGran, coarserSlot, shard));
        Assert.assertNotNull(stamp);
        Assert.assertEquals(UpdateStamp.State.Active, stamp.getState());
        Assert.assertEquals(updateTime, stamp.getTimestamp());
        Assert.assertTrue(stamp.isDirty());
    }
}
