package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ScheduleContextUpdateTest {

    private static List<Integer> shards = new ArrayList<Integer>() {{ add(shard); }};
    private static int shard = 0;

    long now;
    ScheduleContext ctx;
    ShardStateManager mgr;
    SlotKey slotkey5;
    SlotKey slotkey20;
    SlotKey slotkey60;
    SlotKey slotkey240;
    SlotKey slotkey1440;

    @Before
    public void setUp() {

        now = 1234000L;
        ctx = new ScheduleContext(now, shards);
        mgr = ctx.getShardStateManager();
        slotkey5 = SlotKey.of(Granularity.MIN_5, 4, shards.get(0));
        slotkey20 = SlotKey.of(Granularity.MIN_20, 1, shards.get(0));
        slotkey60 = SlotKey.of(Granularity.MIN_60, 0, shards.get(0));
        slotkey240 = SlotKey.of(Granularity.MIN_240, 0, shards.get(0));
        slotkey1440 = SlotKey.of(Granularity.MIN_1440, 0, shards.get(0));

        Assert.assertEquals(0, ctx.getScheduledCount());
        Assert.assertNull(mgr.getUpdateStamp(slotkey5));
        Assert.assertNull(mgr.getUpdateStamp(slotkey20));
        Assert.assertNull(mgr.getUpdateStamp(slotkey60));
        Assert.assertNull(mgr.getUpdateStamp(slotkey240));
        Assert.assertNull(mgr.getUpdateStamp(slotkey1440));
    }

    @Test
    public void testUpdateCreatesActiveDirtyStamp() {

        // given
        Assert.assertNull(mgr.getUpdateStamp(slotkey5));

        // when
        ctx.update(now, shards.get(0));

        // then
        UpdateStamp stamp = ctx.getShardStateManager().getUpdateStamp(slotkey5);
        assertStampStateAndTimestampAndDirtyFlag(stamp, UpdateStamp.State.Active, now, true);
        Assert.assertEquals(0, ctx.getScheduledCount());
    }

    @Test
    public void testUpdateMarksSlotsDirtyAtAllGranularities() {

        // given
        Assert.assertNull(mgr.getUpdateStamp(slotkey5));
        Assert.assertNull(mgr.getUpdateStamp(slotkey20));
        Assert.assertNull(mgr.getUpdateStamp(slotkey60));
        Assert.assertNull(mgr.getUpdateStamp(slotkey240));
        Assert.assertNull(mgr.getUpdateStamp(slotkey1440));

        // when
        ctx.update(now, shards.get(0));

        // then
        UpdateStamp stamp;

        stamp = mgr.getUpdateStamp(slotkey5);
        assertStampStateAndTimestampAndDirtyFlag(stamp, UpdateStamp.State.Active, now, true);

        stamp = mgr.getUpdateStamp(slotkey20);
        assertStampStateAndTimestampAndDirtyFlag(stamp, UpdateStamp.State.Active, now, true);

        stamp = mgr.getUpdateStamp(slotkey60);
        assertStampStateAndTimestampAndDirtyFlag(stamp, UpdateStamp.State.Active, now, true);

        stamp = mgr.getUpdateStamp(slotkey240);
        assertStampStateAndTimestampAndDirtyFlag(stamp, UpdateStamp.State.Active, now, true);

        stamp = mgr.getUpdateStamp(slotkey1440);
        assertStampStateAndTimestampAndDirtyFlag(stamp, UpdateStamp.State.Active, now, true);

        Assert.assertEquals(0, ctx.getScheduledCount());
    }

    private void assertStampStateAndTimestampAndDirtyFlag(UpdateStamp stamp, UpdateStamp.State state, long timestamp, boolean dirty) {

        Assert.assertNotNull(stamp);
        Assert.assertEquals(state, stamp.getState());
        Assert.assertEquals(timestamp, stamp.getTimestamp());
        Assert.assertEquals(dirty, stamp.isDirty());
    }
}
