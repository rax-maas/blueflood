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

    @Before
    public void setUp() {
    }

    @Test
    public void testPushBackToScheduledIncrementsScheduledCount() {

        // given
        long now = 1234000L;
        long updateTime = now - 2;
        Granularity gran = Granularity.MIN_5;
        int slot = gran.slot(now);

        ScheduleContext ctx = new ScheduleContext(now, shards);
        ctx.update(updateTime, shard);
        ctx.scheduleSlotsOlderThan(1);

        // precondition
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
        long now = 1234000L;
        long updateTime = now - 2;
        Granularity gran = Granularity.MIN_5;
        int slot = gran.slot(now);

        ScheduleContext ctx = new ScheduleContext(now, shards);
        ctx.update(updateTime, shard);
        ctx.scheduleSlotsOlderThan(1);

        // precondition
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
        long now = 1234000L;
        long updateTime = now - 2;
        Granularity gran = Granularity.MIN_5;
        int slot = gran.slot(now);
        Granularity coarserGran = null;
        try {
            coarserGran = gran.coarser();
        } catch (GranularityException e) {
            Assert.fail("Couldn't get the next coarser granularity");
        }
        int coarserSlot = coarserGran.slot(updateTime);

        ScheduleContext ctx = new ScheduleContext(now, shards);
        ShardStateManager mgr = ctx.getShardStateManager();
        ctx.update(updateTime, shard);
        ctx.scheduleSlotsOlderThan(1);

        // precondition
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
