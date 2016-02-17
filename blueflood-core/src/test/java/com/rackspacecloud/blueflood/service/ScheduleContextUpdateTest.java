package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.exceptions.GranularityException;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ScheduleContextUpdateTest {

    private static List<Integer> ringShards;

    @Before
    public void setUp() {
        ringShards = new ArrayList<Integer>() {{ add(0); }};
    }

    @Test
    public void testUpdateCreatesActiveDirtyStamp() {

        long now = 1234000L;
        ScheduleContext ctx = new ScheduleContext(now, ringShards);
        SlotKey slotkey = SlotKey.of(Granularity.MIN_5, 4, ringShards.get(0));

        // precondition
        Assert.assertEquals(0, ctx.getScheduledCount());
        UpdateStamp stamp = ctx.getShardStateManager().getUpdateStamp(slotkey);
        Assert.assertNull(stamp);

        // when
        ctx.update(now, ringShards.get(0));

        // then
        stamp = ctx.getShardStateManager().getUpdateStamp(slotkey);
        Assert.assertNotNull(stamp);
        Assert.assertEquals(UpdateStamp.State.Active, stamp.getState());
        Assert.assertEquals(now, stamp.getTimestamp());
        Assert.assertTrue(stamp.isDirty());
        Assert.assertEquals(0, ctx.getScheduledCount());
    }

    @Test
    public void testUpdateMarksSlotsDirtyAtAllGranularities() {

        long now = 1234000L;
        ScheduleContext ctx = new ScheduleContext(now, ringShards);
        ShardStateManager mgr = ctx.getShardStateManager();
        SlotKey slotkey5 = SlotKey.of(Granularity.MIN_5, 4, ringShards.get(0));
        SlotKey slotkey20 = SlotKey.of(Granularity.MIN_20, 1, ringShards.get(0));
        SlotKey slotkey60 = SlotKey.of(Granularity.MIN_60, 0, ringShards.get(0));
        SlotKey slotkey240 = SlotKey.of(Granularity.MIN_240, 0, ringShards.get(0));
        SlotKey slotkey1440 = SlotKey.of(Granularity.MIN_1440, 0, ringShards.get(0));

        // precondition
        Assert.assertEquals(0, ctx.getScheduledCount());
        Assert.assertNull(mgr.getUpdateStamp(slotkey5));
        Assert.assertNull(mgr.getUpdateStamp(slotkey20));
        Assert.assertNull(mgr.getUpdateStamp(slotkey60));
        Assert.assertNull(mgr.getUpdateStamp(slotkey240));
        Assert.assertNull(mgr.getUpdateStamp(slotkey1440));

        // when
        ctx.update(now, ringShards.get(0));

        // then
        UpdateStamp stamp;

        stamp = mgr.getUpdateStamp(slotkey5);
        Assert.assertNotNull(stamp);
        Assert.assertEquals(UpdateStamp.State.Active, stamp.getState());
        Assert.assertEquals(now, stamp.getTimestamp());
        Assert.assertTrue(stamp.isDirty());

        stamp = mgr.getUpdateStamp(slotkey20);
        Assert.assertNotNull(stamp);
        Assert.assertEquals(UpdateStamp.State.Active, stamp.getState());
        Assert.assertEquals(now, stamp.getTimestamp());
        Assert.assertTrue(stamp.isDirty());

        stamp = mgr.getUpdateStamp(slotkey60);
        Assert.assertNotNull(stamp);
        Assert.assertEquals(UpdateStamp.State.Active, stamp.getState());
        Assert.assertEquals(now, stamp.getTimestamp());
        Assert.assertTrue(stamp.isDirty());

        stamp = mgr.getUpdateStamp(slotkey240);
        Assert.assertNotNull(stamp);
        Assert.assertEquals(UpdateStamp.State.Active, stamp.getState());
        Assert.assertEquals(now, stamp.getTimestamp());
        Assert.assertTrue(stamp.isDirty());

        stamp = mgr.getUpdateStamp(slotkey1440);
        Assert.assertNotNull(stamp);
        Assert.assertEquals(UpdateStamp.State.Active, stamp.getState());
        Assert.assertEquals(now, stamp.getTimestamp());
        Assert.assertTrue(stamp.isDirty());

        Assert.assertEquals(0, ctx.getScheduledCount());
    }
}
