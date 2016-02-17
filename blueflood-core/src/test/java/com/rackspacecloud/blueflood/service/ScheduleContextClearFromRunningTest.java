package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.exceptions.GranularityException;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ScheduleContextClearFromRunningTest {

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
        ctx.scheduleSlotsOlderThan(1);
    }

    @Test
    public void testClearFromRunningDecrementsRunningCount() {

        // given
        SlotKey next = ctx.getNextScheduled();
        Assert.assertEquals(1, ctx.getRunningCount());

        // when
        ctx.clearFromRunning(next);

        // then
        Assert.assertEquals(0, ctx.getRunningCount());
    }

    @Test
    public void testClearFromRunningDoesntChangeScheduledCount() {

        // given
        SlotKey next = ctx.getNextScheduled();
        Assert.assertEquals(0, ctx.getScheduledCount());

        // when
        ctx.clearFromRunning(next);

        // then
        Assert.assertEquals(0, ctx.getScheduledCount());
    }

    @Test
    public void testClearFromRunningChangesStateToRolled() {

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
        ctx.clearFromRunning(next);

        // then
        stamp = mgr.getUpdateStamp(next);
        Assert.assertNotNull(stamp);
        Assert.assertEquals(UpdateStamp.State.Rolled, stamp.getState());
        Assert.assertEquals(updateTime, stamp.getTimestamp());
        Assert.assertTrue(stamp.isDirty());

        stamp = mgr.getUpdateStamp(SlotKey.of(coarserGran, coarserSlot, shard));
        Assert.assertNotNull(stamp);
        Assert.assertEquals(UpdateStamp.State.Active, stamp.getState());
        Assert.assertEquals(updateTime, stamp.getTimestamp());
        Assert.assertTrue(stamp.isDirty());
    }
}
