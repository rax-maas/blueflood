package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.exceptions.GranularityException;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ScheduleContextScheduleSlotsOlderThanTest {

    private static List<Integer> shards = new ArrayList<Integer>() {{ add(shard); }};
    private static int shard = 0;

    @Before
    public void setUp() {
    }

    @Test
    public void testScheduleSlotsOlderThanAddsToScheduledCount() {

        long now = 1234000L;
        ScheduleContext ctx = new ScheduleContext(now, shards);
        ctx.update(now - 2, shards.get(0));

        // precondition
        Assert.assertEquals(0, ctx.getScheduledCount());

        // when
        ctx.scheduleSlotsOlderThan(1);

        // then
        Assert.assertEquals(1, ctx.getScheduledCount());
    }

    @Test
    public void testScheduleSlotsOlderThanSetsHasScheduled() {

        long now = 1234000L;
        ScheduleContext ctx = new ScheduleContext(now, shards);
        ctx.update(now - 2, shards.get(0));

        // precondition
        Assert.assertFalse(ctx.hasScheduled());

        // when
        ctx.scheduleSlotsOlderThan(1);

        // then
        Assert.assertTrue(ctx.hasScheduled());
    }

    @Test
    public void testGetNextScheduledDecrementsScheduledCount() {

        // given
        long now = 1234000L;
        ScheduleContext ctx = new ScheduleContext(now, shards);
        int shard = shards.get(0);
        ctx.update(now-2, shard);
        ctx.scheduleSlotsOlderThan(1);

        // precondition
        Assert.assertEquals(1, ctx.getScheduledCount());

        // when
        SlotKey next = ctx.getNextScheduled();

        // then
        Assert.assertEquals(0, ctx.getScheduledCount());
    }

    @Test
    public void testGetNextScheduledIncrementsRunningCount() {

        // given
        long now = 1234000L;
        ScheduleContext ctx = new ScheduleContext(now, shards);
        int shard = shards.get(0);
        ctx.update(now-2, shard);
        ctx.scheduleSlotsOlderThan(1);

        // precondition
        Assert.assertEquals(0, ctx.getRunningCount());

        // when
        SlotKey next = ctx.getNextScheduled();

        // then
        Assert.assertEquals(1, ctx.getRunningCount());
    }

    @Test
    public void testGetNextScheduledReturnsTheSmallestGranularity() {

        // given
        long now = 1234000L;
        long updateTime = now - 2;
        int shard = shards.get(0);
        Granularity gran = Granularity.MIN_5;
        int slot = gran.slot(now);

        ScheduleContext ctx = new ScheduleContext(now, shards);
        ctx.update(updateTime, shard);
        ctx.scheduleSlotsOlderThan(1);

        // when
        SlotKey next = ctx.getNextScheduled();

        // then
        Assert.assertEquals(shard, next.getShard());
        Assert.assertEquals(slot, next.getSlot());
        Assert.assertEquals(gran, next.getGranularity());
    }

    @Test
    public void testGetNextScheduledChangesStateToRunning() {

        // given
        long now = 1234000L;
        long updateTime = now - 2;
        int shard = shards.get(0);
        Granularity gran = Granularity.MIN_5;
        int slot = gran.slot(updateTime);
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
        UpdateStamp stamp = mgr.getUpdateStamp(SlotKey.of(gran, slot, shard));
        Assert.assertNotNull(stamp);
        Assert.assertEquals(UpdateStamp.State.Active, stamp.getState());
        Assert.assertEquals(updateTime, stamp.getTimestamp());
        Assert.assertTrue(stamp.isDirty());

        stamp = mgr.getUpdateStamp(SlotKey.of(coarserGran, coarserSlot, shard));
        Assert.assertNotNull(stamp);
        Assert.assertEquals(UpdateStamp.State.Active, stamp.getState());
        Assert.assertEquals(updateTime, stamp.getTimestamp());
        Assert.assertTrue(stamp.isDirty());

        // when
        SlotKey next = ctx.getNextScheduled();

        // then
        stamp = mgr.getUpdateStamp(next);
        Assert.assertNotNull(stamp);
        Assert.assertEquals(UpdateStamp.State.Running, stamp.getState());
        Assert.assertEquals(updateTime, stamp.getTimestamp());
        Assert.assertTrue(stamp.isDirty());

        stamp = mgr.getUpdateStamp(SlotKey.of(coarserGran, coarserSlot, shard));
        Assert.assertNotNull(stamp);
        Assert.assertEquals(UpdateStamp.State.Active, stamp.getState());
        Assert.assertEquals(updateTime, stamp.getTimestamp());
        Assert.assertTrue(stamp.isDirty());
    }
}
