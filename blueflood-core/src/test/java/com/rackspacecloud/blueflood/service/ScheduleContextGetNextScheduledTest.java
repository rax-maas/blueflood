package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.exceptions.GranularityException;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ScheduleContextGetNextScheduledTest {

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
        shard = shards.get(0);
        gran = Granularity.MIN_5;
        slot = gran.slot(updateTime);
        coarserGran = gran.coarser();
        coarserSlot = coarserGran.slot(updateTime);

        ctx = new ScheduleContext(now, shards);
        mgr = ctx.getShardStateManager();
        ctx.update(updateTime, shard);
        ctx.scheduleEligibleSlots(1, 7200000, 3600000);
    }

    @Test
    public void testGetNextScheduledDecrementsScheduledCount() {

        // given
        Assert.assertEquals(1, ctx.getScheduledCount());

        // when
        SlotKey next = ctx.getNextScheduled();

        // then
        Assert.assertEquals(0, ctx.getScheduledCount());
    }

    @Test
    public void testGetNextScheduledIncrementsRunningCount() {

        // given
        Assert.assertEquals(0, ctx.getRunningCount());

        // when
        SlotKey next = ctx.getNextScheduled();

        // then
        Assert.assertEquals(1, ctx.getRunningCount());
    }

    @Test
    public void testGetNextScheduledReturnsTheSmallestGranularity() {

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
