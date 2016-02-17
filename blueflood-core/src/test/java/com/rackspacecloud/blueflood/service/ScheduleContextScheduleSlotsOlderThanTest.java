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

    long now;
    ScheduleContext ctx;

    @Before
    public void setUp() {

        now = 1234000L;
        ctx = new ScheduleContext(now, shards);
        ctx.update(now - 2, shards.get(0));
    }

    @Test
    public void testScheduleSlotsOlderThanAddsToScheduledCount() {

        // given
        Assert.assertEquals(0, ctx.getScheduledCount());

        // when
        ctx.scheduleSlotsOlderThan(1);

        // then
        Assert.assertEquals(1, ctx.getScheduledCount());
    }

    @Test
    public void testScheduleSlotsOlderThanSetsHasScheduled() {

        // given
        Assert.assertFalse(ctx.hasScheduled());

        // when
        ctx.scheduleSlotsOlderThan(1);

        // then
        Assert.assertTrue(ctx.hasScheduled());
    }
}
