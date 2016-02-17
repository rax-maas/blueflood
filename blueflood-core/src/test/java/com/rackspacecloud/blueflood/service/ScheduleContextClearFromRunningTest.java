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

    private static List<Integer> ringShards;

    @Before
    public void setUp() {
        ringShards = new ArrayList<Integer>() {{ add(0); }};
    }

    @Test
    public void testClearFromRunningDecrementsRunningCount() {

        // given
        long now = 1234000L;
        long updateTime = now - 2;
        int shard = ringShards.get(0);

        ScheduleContext ctx = new ScheduleContext(now, ringShards);
        ctx.update(updateTime, shard);
        ctx.scheduleSlotsOlderThan(1);

        // precondition
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
        long now = 1234000L;
        long updateTime = now - 2;
        int shard = ringShards.get(0);

        ScheduleContext ctx = new ScheduleContext(now, ringShards);
        ctx.update(updateTime, shard);
        ctx.scheduleSlotsOlderThan(1);

        // precondition
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
        long now = 1234000L;
        long updateTime = now - 2;
        int shard = ringShards.get(0);
        Granularity gran = Granularity.MIN_5;
        int slot = gran.slot(now);
        Granularity coarserGran = null;
        try {
            coarserGran = gran.coarser();
        } catch (GranularityException e) {
            Assert.fail("Couldn't get the next coarser granularity");
        }
        int coarserSlot = coarserGran.slot(updateTime);

        ScheduleContext ctx = new ScheduleContext(now, ringShards);
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
