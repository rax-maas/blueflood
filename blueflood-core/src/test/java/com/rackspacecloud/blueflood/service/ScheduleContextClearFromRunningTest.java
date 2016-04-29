package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.exceptions.GranularityException;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import com.rackspacecloud.blueflood.utils.Clock;
import org.joda.time.Instant;
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

    final long lastRollupTime = System.currentTimeMillis();

    @Before
    public void setUp() throws GranularityException {

        now = 1234000L;
        updateTime = now - 2;
        gran = Granularity.MIN_5;
        slot = gran.slot(now);
        coarserGran = gran.coarser();
        coarserSlot = coarserGran.slot(updateTime);

        ctx = new ScheduleContext(now, shards, new Clock() {
            @Override
            public Instant now() {
                return new Instant(lastRollupTime);
            }
        });
        mgr = ctx.getShardStateManager();
        ctx.update(updateTime, shard);
        ctx.scheduleEligibleSlots(1, 7200000, 3600000);
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
        Assert.assertEquals("last rollup time", lastRollupTime, stamp.getLastRollupTimestamp());

        stamp = mgr.getUpdateStamp(SlotKey.of(coarserGran, coarserSlot, shard));
        Assert.assertNotNull(stamp);
        Assert.assertEquals(UpdateStamp.State.Active, stamp.getState());
        Assert.assertEquals(updateTime, stamp.getTimestamp());
        Assert.assertTrue(stamp.isDirty());
        Assert.assertEquals("last rollup time", 0, stamp.getLastRollupTimestamp());
    }

    @Test
    public void testClearFromRunningSetsLastRollupTime() {

        // given
        SlotKey next = ctx.getNextScheduled();

        // when
        ctx.clearFromRunning(next);

        // then
        UpdateStamp stamp = mgr.getUpdateStamp(next);
        Assert.assertNotNull(stamp);
        Assert.assertEquals(UpdateStamp.State.Rolled, stamp.getState());
        Assert.assertEquals(updateTime, stamp.getTimestamp());
        Assert.assertTrue(stamp.isDirty());
        Assert.assertEquals("last rollup time", lastRollupTime, stamp.getLastRollupTimestamp());
    }

    @Test
    public void testClearFromRunningSetsLastRollupTimeEvenWhenSlotBecomesActive() {
        //slot became active before Rolled state can be saved to db because of delayed metric.

        // given
        SlotKey next = ctx.getNextScheduled();

        UpdateStamp stamp = mgr.getUpdateStamp(SlotKey.of(gran, slot, shard));
        stamp.setState(UpdateStamp.State.Active); //delayed metric changed state to Active

        // when
        ctx.clearFromRunning(next);

        // then
        stamp = mgr.getUpdateStamp(next);
        Assert.assertNotNull(stamp);
        Assert.assertEquals(UpdateStamp.State.Active, stamp.getState());
        Assert.assertEquals(updateTime, stamp.getTimestamp());
        Assert.assertTrue(stamp.isDirty());
        Assert.assertEquals("last rollup time", lastRollupTime, stamp.getLastRollupTimestamp());
    }
}
