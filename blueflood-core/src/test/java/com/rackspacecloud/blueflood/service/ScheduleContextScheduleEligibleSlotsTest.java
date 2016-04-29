package com.rackspacecloud.blueflood.service;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ScheduleContextScheduleEligibleSlotsTest {

    private static List<Integer> shards = new ArrayList<Integer>() {{ add(shard); }};
    private static int shard = 0;

    long now;
    ScheduleContext ctx;

    @Before
    public void setUp() {

        now = 1234000L;
        ctx = new ScheduleContext(now, shards);
        ctx.update(now - 2, shards.get(0));
    }

    @Test
    public void testScheduleEligibleSlotsAddsToScheduledCount() {

        // given
        Assert.assertEquals(0, ctx.getScheduledCount());

        // when
        ctx.scheduleEligibleSlots(1, 7200000, 3600000);

        // then
        Assert.assertEquals(1, ctx.getScheduledCount());
    }

    @Test
    public void testScheduleEligibleSlotsSetsHasScheduled() {

        // given
        Assert.assertFalse(ctx.hasScheduled());

        // when
        ctx.scheduleEligibleSlots(1, 7200000, 3600000);

        // then
        Assert.assertTrue(ctx.hasScheduled());
    }
}
