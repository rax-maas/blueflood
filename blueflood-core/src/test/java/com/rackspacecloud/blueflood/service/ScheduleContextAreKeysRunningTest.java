package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ScheduleContextAreKeysRunningTest {

    long currentTime;
    final int shard = 0;
    List<Integer> managedShards;
    ScheduleContext ctx;
    int slot;
    SlotKey slotkey;

    private final int SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS = 7200000;
    private final int LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS = 3600000;

    @Before
    public void setUp() {

        // given
        currentTime = 1234000L;

        managedShards = new ArrayList<Integer>() {{ add(shard); }};

        ctx = new ScheduleContext(currentTime, managedShards);

        slot = Granularity.MIN_5.slot(currentTime);
        slotkey = SlotKey.of(Granularity.MIN_5, slot, shard);
    }

    @Test
    public void noneScheduledOrRunningReturnsFalse() {

        // precondition
        assertEquals(0, ctx.getScheduledCount());
        assertEquals(0, ctx.getRunningCount());

        // when
        boolean areKeysRunning = ctx.areChildKeysOrSelfKeyScheduledOrRunning(slotkey);

        // then
        assertFalse(areKeysRunning);
    }

    @Test
    public void slotScheduledReturnsTrue() {

        // given
        ctx.update(currentTime - 2, shard);
        ctx.scheduleEligibleSlots(1, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);

        // precondition
        assertEquals(1, ctx.getScheduledCount());
        assertEquals(0, ctx.getRunningCount());

        // when
        boolean areKeysRunning = ctx.areChildKeysOrSelfKeyScheduledOrRunning(slotkey);

        // then
        assertTrue(areKeysRunning);
    }

    @Test
    public void childSlotScheduledReturnsTrue() {

        // given
        ctx.update(currentTime - 2, shard);
        ctx.scheduleEligibleSlots(1, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);

        int otherSlot = Granularity.MIN_20.slot(currentTime);
        SlotKey otherSlotkey = SlotKey.of(Granularity.MIN_20, otherSlot, shard);

        // precondition
        assertEquals(1, ctx.getScheduledCount());
        assertEquals(0, ctx.getRunningCount());

        // when
        boolean areKeysRunning = ctx.areChildKeysOrSelfKeyScheduledOrRunning(otherSlotkey);

        // then
        assertTrue(areKeysRunning);
    }

    @Test
    public void unrelatedSlotScheduledReturnsFalse() {

        // given
        ctx.update(currentTime - 2, shard);
        ctx.scheduleEligibleSlots(1, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);

        int otherSlot = Granularity.MIN_5.slot(currentTime - 5*60*1000); // check the previous slot from 5 minutes ago
        SlotKey otherSlotkey = SlotKey.of(Granularity.MIN_5, otherSlot, shard);

        // precondition
        assertEquals(1, ctx.getScheduledCount());
        assertEquals(0, ctx.getRunningCount());

        // when
        boolean areKeysRunning = ctx.areChildKeysOrSelfKeyScheduledOrRunning(otherSlotkey);

        // then
        assertFalse(areKeysRunning);
    }

    @Test
    public void slotRunningReturnsTrue() {

        // given
        ctx.update(currentTime - 2, shard);
        ctx.scheduleEligibleSlots(1, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);

        SlotKey runningSlot = ctx.getNextScheduled();

        // precondition
        assertEquals(0, ctx.getScheduledCount());
        assertEquals(1, ctx.getRunningCount());
        assertEquals(slotkey, runningSlot);

        // when
        boolean areKeysRunning = ctx.areChildKeysOrSelfKeyScheduledOrRunning(slotkey);

        // then
        assertTrue(areKeysRunning);
    }

    @Test
    public void childSlotRunningReturnsTrue() {

        // given
        ctx.update(currentTime - 2, shard);
        ctx.scheduleEligibleSlots(1, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);

        int otherSlot = Granularity.MIN_20.slot(currentTime);
        SlotKey otherSlotkey = SlotKey.of(Granularity.MIN_20, otherSlot, shard);

        SlotKey runningSlot = ctx.getNextScheduled();

        // precondition
        assertEquals(0, ctx.getScheduledCount());
        assertEquals(1, ctx.getRunningCount());

        // when
        boolean areKeysRunning = ctx.areChildKeysOrSelfKeyScheduledOrRunning(otherSlotkey);

        // then
        assertTrue(areKeysRunning);
    }

    @Test
    public void slotPushedBackReturnsTrue() {

        // given
        ctx.update(currentTime - 2, shard);
        ctx.scheduleEligibleSlots(1, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);

        SlotKey runningSlot = ctx.getNextScheduled();

        // precondition
        assertEquals(0, ctx.getScheduledCount());
        assertEquals(1, ctx.getRunningCount());

        ctx.clearFromRunning(runningSlot);
        ctx.pushBackToScheduled(runningSlot, false);

        // precondition
        assertEquals(1, ctx.getScheduledCount());
        assertEquals(0, ctx.getRunningCount());

        // when
        boolean areKeysRunning = ctx.areChildKeysOrSelfKeyScheduledOrRunning(slotkey);

        // then
        assertTrue(areKeysRunning);
    }

    @Test
    public void childSlotPushedBackAfterClearFromRunningReturnsTrue() {

        // given
        ctx.update(currentTime - 2, shard);
        ctx.scheduleEligibleSlots(1, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);

        SlotKey runningSlot = ctx.getNextScheduled();

        int otherSlot = Granularity.MIN_20.slot(currentTime);
        SlotKey otherSlotkey = SlotKey.of(Granularity.MIN_20, otherSlot, shard);

        // precondition
        assertEquals(0, ctx.getScheduledCount());
        assertEquals(1, ctx.getRunningCount());

        // NOTE: pushBackToScheduled does not remove a slot from the running
        // count. In order to get the ScheduleContext into the proper state
        // before areChildKeysOrSelfKeyScheduledOrRunning gets called, we call
        // clearFromRunning here because that method DOES remove the slot from
        // the running category.
        // Calling clearFromRunning and pushBackToScheduled in succession like
        // this is not normal. It is done here simply to demonstrate existing
        // behavior.
        ctx.clearFromRunning(runningSlot);

        // precondition
        assertEquals(0, ctx.getScheduledCount());
        assertEquals(0, ctx.getRunningCount());

        ctx.pushBackToScheduled(runningSlot, false);

        // precondition
        assertEquals(1, ctx.getScheduledCount());
        assertEquals(0, ctx.getRunningCount());

        // when
        boolean areKeysRunning = ctx.areChildKeysOrSelfKeyScheduledOrRunning(otherSlotkey);

        // then
        assertTrue(areKeysRunning);
    }

    @Test
    public void childSlotPushedBackReturnsTrue() {

        // given
        ctx.update(currentTime - 2, shard);
        ctx.scheduleEligibleSlots(1, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);

        SlotKey runningSlot = ctx.getNextScheduled();

        int otherSlot = Granularity.MIN_20.slot(currentTime);
        SlotKey otherSlotkey = SlotKey.of(Granularity.MIN_20, otherSlot, shard);

        // precondition
        assertEquals(0, ctx.getScheduledCount());
        assertEquals(1, ctx.getRunningCount());

        ctx.pushBackToScheduled(runningSlot, false);

        // precondition
        assertEquals(1, ctx.getScheduledCount());
        assertEquals(1, ctx.getRunningCount());

        // when
        boolean areKeysRunning = ctx.areChildKeysOrSelfKeyScheduledOrRunning(otherSlotkey);

        // then
        assertTrue(areKeysRunning);
    }
}
