/*
 * Copyright 2013 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import com.rackspacecloud.blueflood.utils.Clock;
import com.rackspacecloud.blueflood.utils.Util;
import com.rackspacecloud.blueflood.utils.TimeValue;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.when;

public class ScheduleContextTest {
    private static final Logger log = LoggerFactory.getLogger("tests");
    private static List<Integer> shards = new ArrayList<Integer>() {{ add(shard); }};
    private static int shard = 0;
    private static final TimeValue MULTI_THREAD_SOFT_TIMEOUT = new TimeValue(60000L, TimeUnit.MILLISECONDS);;

    private final long ROLLUP_DELAY_MILLIS = 300000; //5 mins
    private final long SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS = 1800000; //30 mins
    private final long LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS = 1800000; //15 mins

    @Before
    public void setUp() {
    }

    @Test
    public void testSimpleUpdateAndSchedule() {
        long clock = 1234000L;
        ScheduleContext ctx = new ScheduleContext(clock, shards);
        Collection<SlotKey> scheduled = new ArrayList<SlotKey>();
        Collection<SlotKey> expected = new ArrayList<SlotKey>();

        ctx.setCurrentTimeMillis(clock); // +0m
        ctx.update(clock, shards.get(0));
        ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);
        Assert.assertFalse(ctx.hasScheduled());

        clock += 300000; // +5m
        ctx.setCurrentTimeMillis(clock);
        ctx.update(clock, shards.get(0));
        ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);
        // at +5m nothing should be scheduled.
        Assert.assertFalse(ctx.hasScheduled());

        clock += 300000; // +10m
        ctx.setCurrentTimeMillis(clock);
        ctx.update(clock, shards.get(0));
        ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);
        // at this point, metrics_full,4 should be scheduled, but not metrics_5m,4 even though it is older than 300s.
        // metrics_5m,4 cannot be scheduled because one of its children is scheduled.  once the child is removed and
        // scheduling is re-ran, it should appear though.  The next few lines test those assumptions.

        expected.add(SlotKey.parse("metrics_5m,4,0"));
        while (ctx.hasScheduled())
            scheduled.add(ctx.getNextScheduled());
        Assert.assertEquals(expected, scheduled);
        ctx.clearFromRunning(SlotKey.parse("metrics_5m,4,0"));

        // now, time doesn't change, but we re-evaluate slots that can be scheduled.
        ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);
        Assert.assertFalse(ctx.hasScheduled());
        expected.clear();
        scheduled.clear();

        // technically, we're one second away from when metrics_full,5 and metrics_5m,5 can be scheduled.
        ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);
        Assert.assertFalse(ctx.hasScheduled());

        clock += 1000; // 1s
        ctx.setCurrentTimeMillis(clock);
        ctx.update(clock, shards.get(0));
        ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);
        Assert.assertTrue(ctx.hasScheduled());
        Assert.assertEquals(ctx.getNextScheduled(), SlotKey.parse("metrics_5m,5,0"));
        Assert.assertFalse(ctx.hasScheduled());
        ctx.clearFromRunning(SlotKey.parse("metrics_5m,5,0"));
        ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);
        Assert.assertFalse(ctx.hasScheduled());


        clock += 3600000; // 1h
        ctx.setCurrentTimeMillis(clock);
        ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);
        Assert.assertTrue(ctx.hasScheduled());
        Assert.assertEquals(ctx.getNextScheduled(), SlotKey.parse("metrics_5m,6,0"));
        Assert.assertFalse(ctx.hasScheduled());
        ctx.clearFromRunning(SlotKey.parse("metrics_5m,6,0"));

        // time doesn't change, but now that all the 5m slots have been scheduled, we should start seeing coarser slots
        // available for scheduling.
        ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);
        Assert.assertTrue(ctx.hasScheduled());
        Assert.assertEquals(ctx.getNextScheduled(), SlotKey.parse("metrics_20m,1,0"));
        Assert.assertFalse(ctx.hasScheduled());
        ctx.clearFromRunning(SlotKey.parse("metrics_20m,1,0"));

        // let's finish this off...
        ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);
        Assert.assertTrue(ctx.hasScheduled());
        Assert.assertEquals(SlotKey.parse("metrics_60m,0,0"), ctx.getNextScheduled());
        Assert.assertFalse(ctx.hasScheduled());
        ctx.clearFromRunning(SlotKey.parse("metrics_60m,0,0"));

        ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);
        Assert.assertTrue(ctx.hasScheduled());
        Assert.assertEquals(SlotKey.parse("metrics_240m,0,0"), ctx.getNextScheduled());
        Assert.assertFalse(ctx.hasScheduled());
        ctx.clearFromRunning(SlotKey.parse("metrics_240m,0,0"));

        ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);
        Assert.assertTrue(ctx.hasScheduled());
        Assert.assertEquals(SlotKey.parse("metrics_1440m,0,0"), ctx.getNextScheduled());
        Assert.assertFalse(ctx.hasScheduled());
        ctx.clearFromRunning(SlotKey.parse("metrics_1440m,0,0"));

        ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);
        Assert.assertFalse(ctx.hasScheduled());
    }

    @Test
    public void test48HoursSequential() {
        long clock = 1234000L;
        ScheduleContext ctx = new ScheduleContext(clock, shards);
        int count = 0;

        // every 30s for 48 hrs.
        for (int i = 0; i < 48 * 60 * 60; i += 30) {
            clock += 30000;
            ctx.setCurrentTimeMillis(clock);
            ctx.update(clock, shards.get(0));
        }
        ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);

        // 5m should include slots 4 through 578.
        String prefix = "metrics_5m,";
        for (int i = 4; i <= 578; i++) {
            count++;
            SlotKey key = ctx.getNextScheduled();
            Assert.assertNotNull(key);
            Assert.assertEquals(Granularity.MIN_5, key.getGranularity());
            ctx.clearFromRunning(key);
        }
        ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);

        // 20m 1:143
        prefix = "metrics_20m,";
        for (int i = 1; i <= 143; i++) {
            count++;
            SlotKey key = ctx.getNextScheduled();
            Assert.assertNotNull(key);
            Assert.assertEquals(Granularity.MIN_20, key.getGranularity());
            ctx.clearFromRunning(key);
        }
        ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);

        // 60m 0:47
        prefix = "metrics_60m,";
        for (int i = 0; i <= 47; i++) {
            count++;
            SlotKey key = ctx.getNextScheduled();
            Assert.assertNotNull(key);
            Assert.assertEquals(Granularity.MIN_60, key.getGranularity());
            ctx.clearFromRunning(key);
        }
        ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);

        // 240m 0:11
        prefix = "metrics_240m,";
        for (int i = 0; i <= 11; i++) {
            count++;
            SlotKey key = ctx.getNextScheduled();
            Assert.assertNotNull(key);
            Assert.assertEquals(Granularity.MIN_240, key.getGranularity());
            ctx.clearFromRunning(key);
        }
        ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);

        // 1440m 0:1
        prefix = "metrics_1440m,";
        for (int i = 0; i <= 1; i++) {
            count++;
            SlotKey key = ctx.getNextScheduled();
            Assert.assertNotNull(key);
            Assert.assertEquals(Granularity.MIN_1440, key.getGranularity());
            ctx.clearFromRunning(key);
        }

        // I don't really need to test this here, but it's useful to know where the number in test48HoursInterlaced
        // comes from.
        Assert.assertEquals(575 + 143 + 48 + 12 + 2, count);
        Assert.assertFalse(ctx.hasScheduled());
    }

    // roughly the same test as test48HoursSequential, but we pull slots out in a different order. the count, and thus
    // the state, should should be the same at the end though. This mimics more closely what will happen in production
    // but will be hard to see.  A good example here is that metrics_240m,0 is scheduled right AFTER metrics_60m,3
    // (and naturally after metrics_60m,{0..2}).
    @Test
    public void test48HoursInterlaced() {
        long clock = 1234000L;
        ScheduleContext ctx = new ScheduleContext(clock, shards);

        int count = 0;
        // every 30s for 48 hrs.
        for (int i = 0; i < 48 * 60 * 60; i+= 30) {
            ctx.update(clock, shards.get(0));
            clock += 30000;
            ctx.setCurrentTimeMillis(clock);
            ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);
            while (ctx.hasScheduled()) {
                count++;
                SlotKey key = ctx.getNextScheduled();
                ctx.clearFromRunning(key);
            }
        }
        Assert.assertEquals(575 + 143 + 48 + 12 + 2, count);
    }

    // my purpose is to run constantly and hope for no deadlocks.
    // handy: ps auxww | grep java | grep bundle | awk '{if (NR==1) {print $2}}' | xargs kill -3
    // this test takes about 20s on my machine.
    @Test
    public void testMultithreadedness() {
        final AtomicLong clock = new AtomicLong(1234L);
        final ScheduleContext ctx = new ScheduleContext(clock.get(), shards);
        final CountDownLatch latch = new CountDownLatch(3);
        final AtomicInteger updateCount = new AtomicInteger(0);
        final AtomicInteger scheduleCount = new AtomicInteger(0);
        final AtomicInteger executionCount = new AtomicInteger(0);

        // 70 days of simulation.
        final int days = 35;
        final Thread update = new Thread("Update") { public void run() {
            int count = 0;
            long time = clock.get();  // saves a bit of lock contention.
            for (int i = 0; i < days * 24 * 60 * 60; i += 30) {
                if (latch.getCount() == 0) {
                    break;
                }
                time += 30000;
                clock.set(time);
                ctx.setCurrentTimeMillis(time);
                ctx.update(time, shard);
                count++;
            }
            updateCount.set(count);
            latch.countDown();
        }};

        final Thread schedule = new Thread("Scheduler") { public void run() {
            int count = 0;
            while (update.isAlive()) {
                ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);
                count++;
                // we sleep here because scheduling needs to happen periodically, not continually.  If there were no
                // sleep here the update thread gets starved and has a hard time completing.
                try { sleep(100L); } catch (Exception ex) {}
            }
            scheduleCount.set(count);
            latch.countDown();
        }};

        Thread consume = new Thread("Runner") { public void run() {
            int count = 0;
            while (update.isAlive()) {
                while (ctx.hasScheduled()) {
                    SlotKey key = ctx.getNextScheduled();
                    ctx.clearFromRunning(key);
                    count++;
                }
            }
            executionCount.set(count);
            latch.countDown();
        }};

        final AtomicBoolean softTimeoutReached = new AtomicBoolean(false);
        Timer timer = new Timer("Soft timeout");
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                while (latch.getCount() > 0) {
                    softTimeoutReached.set(true);
                    latch.countDown();
                }
            }
        }, MULTI_THREAD_SOFT_TIMEOUT.toMillis());

        update.start();
        schedule.start();
        consume.start();

        try {
            latch.await();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }

        Assert.assertTrue(updateCount.get() > 0);
        Assert.assertTrue(scheduleCount.get() > 0);
        Assert.assertTrue(executionCount.get() > 0);
        Assert.assertFalse("Soft timeout was reached; deadlock or thread starvation suspected", softTimeoutReached.get());
    }

    @Test
    public void testScheduleYourShardsOnly() {
        long time = 1234000;
        Collection<Integer> shardsA = Lists.newArrayList(0, 1);
        Collection<Integer> shardsB = Lists.newArrayList(2,3,4);
        ScheduleContext ctxA = new ScheduleContext(time, shardsA); // 327,345,444,467,504,543, 32,426,476,571
        ScheduleContext ctxB = new ScheduleContext(time, shardsB); // 184,320,456,526, 435,499, 20,96,107,236,429
        Collection<Integer> allShards = Lists.newArrayList(0, 1, 2, 3, 4);

        time += 1000;
        for (int shardToUpdate : allShards) {
            ctxA.update(time, shardToUpdate);
            ctxB.update(time, shardToUpdate);
        }

        time += 500000;
        ctxA.setCurrentTimeMillis(time);
        ctxB.setCurrentTimeMillis(time);
        ctxA.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);
        ctxB.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);

        Assert.assertTrue(ctxA.hasScheduled());
        while (ctxA.hasScheduled()) {
            int nextScheduledShard = ctxA.getNextScheduled().getShard();
            Assert.assertTrue(shardsA.contains(nextScheduledShard));
            Assert.assertFalse(shardsB.contains(nextScheduledShard));
        }
        Assert.assertTrue(ctxB.hasScheduled());
        while (ctxB.hasScheduled()) {
            int nextScheduledShard = ctxB.getNextScheduled().getShard();
            Assert.assertTrue(shardsB.contains(nextScheduledShard));
            Assert.assertFalse(shardsA.contains(nextScheduledShard));
        }
    }
    
    @Test
    public void testRecentlyScheduledShards() {
        long now = 1234000;
        ScheduleContext ctx = new ScheduleContext(now, Util.parseShards("ALL"));
        // change the cache with one that expires after 1 sec.
        Cache<Integer, Long> expiresQuickly = CacheBuilder.newBuilder().expireAfterWrite(2, TimeUnit.SECONDS).build();
        Whitebox.setInternalState(ctx, "recentlyScheduledShards", expiresQuickly);
        Assert.assertEquals(0, ctx.getScheduledCount());
        
        // update, set time into future, poll (forced scheduling).
        int otherShard = 2;
        now += 1000;
        ctx.update(now, otherShard);
        now += 300001;
        ctx.setCurrentTimeMillis(now);
        ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);
        
        // otherShard should be recently scheduled.
        Assert.assertTrue(ctx.getRecentlyScheduledShards().contains(otherShard));
        
        // wait for expiration, then verify absence.
        try { Thread.sleep(2100); } catch (Exception ex) {}
        Assert.assertFalse(ctx.getRecentlyScheduledShards().contains(otherShard));
    }

    @Test
    public void testGetNextScheduledSecondTimeReturnsNull() {

        // given
        long now = 1234000L;
        long updateTime = now - 2;
        Granularity gran = Granularity.MIN_5;
        int slot = gran.slot(now);

        ScheduleContext ctx = new ScheduleContext(now, shards);
        ctx.update(updateTime, shard);
        ctx.scheduleEligibleSlots(1, 7200000, 3600000);

        // precondition
        SlotKey next = ctx.getNextScheduled();
        Assert.assertNotNull(next);
        Assert.assertEquals(shard, next.getShard());
        Assert.assertEquals(slot, next.getSlot());
        Assert.assertEquals(gran, next.getGranularity());

        // when
        next = ctx.getNextScheduled();

        // then
        Assert.assertNull(next);
    }

    @Test
    public void testGetNextScheduledWhenNoneAreScheduledReturnsNull() {

        // given
        long now = 1234000L;
        ScheduleContext ctx = new ScheduleContext(now, shards);

        // precondition
        Assert.assertEquals(0, ctx.getScheduledCount());

        // when
        SlotKey next = ctx.getNextScheduled();

        // then
        Assert.assertNull(next);
        Assert.assertEquals(0, ctx.getScheduledCount());
    }

    @Test
    public void testPushBackToScheduledOnNonRunningSlotDoesntChangeState() {

        // given
        long now = 1234000L;
        long updateTime = now - 2;
        Granularity gran = Granularity.MIN_5;
        int slot = gran.slot(now);
        SlotKey slotKey = SlotKey.of(gran, slot, shard);

        ScheduleContext ctx = new ScheduleContext(now, shards);
        ShardStateManager mgr = ctx.getShardStateManager();
        ctx.update(updateTime, shard);

        // precondition
        UpdateStamp stamp = mgr.getUpdateStamp(slotKey);
        Assert.assertNotNull(stamp);
        Assert.assertEquals(UpdateStamp.State.Active, stamp.getState());
        Assert.assertEquals(updateTime, stamp.getTimestamp());
        Assert.assertTrue(stamp.isDirty());

        // when
        ctx.pushBackToScheduled(slotKey, true);

        // then
        stamp = mgr.getUpdateStamp(slotKey);
        Assert.assertNotNull(stamp);
        Assert.assertEquals(UpdateStamp.State.Active, stamp.getState());
        Assert.assertEquals(updateTime, stamp.getTimestamp());
        Assert.assertTrue(stamp.isDirty());
    }

    @Test
    public void testPushBackToScheduledOnNonRunningSlot_DOES_ChangeScheduledCount() {

        // TODO: This seems incorrect. If we were to accidentally call
        // pushBackToScheduled on a slot that wasn't running, we would
        // effectively bypass the scheduleEligibleSlots method

        // given
        long now = 1234000L;
        long updateTime = now - 2;
        Granularity gran = Granularity.MIN_5;
        int slot = gran.slot(now);
        SlotKey slotKey = SlotKey.of(gran, slot, shard);

        ScheduleContext ctx = new ScheduleContext(now, shards);
        ctx.update(updateTime, shard);

        // precondition
        Assert.assertEquals(0, ctx.getScheduledCount());

        // when
        ctx.pushBackToScheduled(slotKey, true);

        // then
        Assert.assertEquals(1, ctx.getScheduledCount());
    }

    @Test
    public void testPushBackToScheduledOnNonRunningSlotDoesntChangeRunningCount() {

        // given
        long now = 1234000L;
        long updateTime = now - 2;
        Granularity gran = Granularity.MIN_5;
        int slot = gran.slot(now);
        SlotKey slotKey = SlotKey.of(gran, slot, shard);

        ScheduleContext ctx = new ScheduleContext(now, shards);
        ctx.update(updateTime, shard);

        // precondition
        Assert.assertEquals(0, ctx.getRunningCount());

        // when
        ctx.pushBackToScheduled(slotKey, true);

        // then
        Assert.assertEquals(0, ctx.getRunningCount());
    }

    @Test(expected=NullPointerException.class)
    public void testPushBackToScheduledWithoutFirstUpdatingThrowsException() {

        // TODO: This appears to be due to the
        // SlotStateManager.slotToUpdateStampMap map not being populated until
        // update() is called.

        // given
        long now = 1234000L;
        Granularity gran = Granularity.MIN_5;
        int slot = gran.slot(now);
        SlotKey slotKey = SlotKey.of(gran, slot, shard);

        ScheduleContext ctx = new ScheduleContext(now, shards);
        //ctx.update(updateTime, shard);

        // precondition
        Assert.assertEquals(0, ctx.getScheduledCount());
        Assert.assertEquals(0, ctx.getRunningCount());

        // when
        ctx.pushBackToScheduled(slotKey, true);

        // then
        // an NPE is thrown
    }

    @Test
    public void testSimpleUpdateAndScheduleWithDelayedMetrics() {
        //testing for one slot -> 0

        final long initialCollectionTime = 1209600000000L;
        //***************************************************************************************
        //*  Testing for on-time metrics (ROLLUP_DELAY_MILLIS)                                  *
        //***************************************************************************************

        long clock = initialCollectionTime;
        final Clock mockClock = Mockito.mock(Clock.class);

        ScheduleContext ctx = new ScheduleContext(clock, shards, mockClock);
        Collection<SlotKey> scheduled = new ArrayList<SlotKey>();
        Collection<SlotKey> expected = new ArrayList<SlotKey>();

        ctx.setCurrentTimeMillis(clock); // +0m
        when(mockClock.now()).thenReturn(new Instant(initialCollectionTime + 1));
        ctx.update(clock, shards.get(0)); //metric ingestion

        clock = initialCollectionTime + ROLLUP_DELAY_MILLIS + 1; // a little bit over +5m
        ctx.setCurrentTimeMillis(clock);
        ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);

        expected.add(SlotKey.parse("metrics_5m,0,0"));
        while (ctx.hasScheduled())
            scheduled.add(ctx.getNextScheduled());
        Assert.assertEquals(expected, scheduled);

        long lastRollupTime = clock;
        when(mockClock.now()).thenReturn(new Instant(lastRollupTime));
        ctx.clearFromRunning(SlotKey.parse("metrics_5m,0,0")); //metrics_5m,0 is now rolled up.
        ctx.clearFromRunning(SlotKey.parse("metrics_20m,0,0"));
        ctx.clearFromRunning(SlotKey.parse("metrics_60m,0,0"));
        ctx.clearFromRunning(SlotKey.parse("metrics_240m,0,0"));
        ctx.clearFromRunning(SlotKey.parse("metrics_1440m,0,0"));

        scheduled.clear();

        //****************************************************************************************
        //*  Testing for short delay metrics(SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS)            *
        //****************************************************************************************

        //delayed metrics for slot 0 (metrics sent after ROLLUP_DELAY_MILLIS)
        long lastIngestTime1 = initialCollectionTime + ROLLUP_DELAY_MILLIS + 300000;
        when(mockClock.now()).thenReturn(new Instant(lastIngestTime1));
        long collectionTime1 = initialCollectionTime + 1;
        ctx.update(collectionTime1, shards.get(0)); //metric ingestion
        ctx.setCurrentTimeMillis(lastIngestTime1);
        ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);

        Assert.assertFalse("Rollup should not be scheduled for re-roll", ctx.hasScheduled());

        //Trying for rollup again after SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS past since last collection time
        clock = collectionTime1 + SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS + 1; //a little bit over 30 mins
        ctx.setCurrentTimeMillis(clock);
        ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);

        while (ctx.hasScheduled())
            scheduled.add(ctx.getNextScheduled());

        Assert.assertEquals(expected, scheduled);
        ctx.clearFromRunning(SlotKey.parse("metrics_5m,0,0")); //metrics_5m,0 is now rolled up.
        ctx.clearFromRunning(SlotKey.parse("metrics_20m,0,0"));
        ctx.clearFromRunning(SlotKey.parse("metrics_60m,0,0"));
        ctx.clearFromRunning(SlotKey.parse("metrics_240m,0,0"));
        ctx.clearFromRunning(SlotKey.parse("metrics_1440m,0,0"));

        scheduled.clear();

        //****************************************************************************************
        //*  Testing for long delay metrics(LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS)               *
        //****************************************************************************************

        //delayed metrics with longer delay for slot 0 (metrics sent after SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS)
        long lastIngestTime2 = initialCollectionTime + SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS + 300000;
        when(mockClock.now()).thenReturn(new Instant(lastIngestTime2));
        long collectionTime2 = initialCollectionTime + 2;
        ctx.update(collectionTime2, shards.get(0)); //metric ingestion
        ctx.setCurrentTimeMillis(lastIngestTime2 + 1);
        ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);

        Assert.assertFalse("Rollup should not be scheduled for re-roll cos of rollup_wait", ctx.hasScheduled());

        //Trying for rollup again after SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS past since last collection time
        clock = lastIngestTime2 + LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS + 1;
        ctx.setCurrentTimeMillis(clock);
        ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);

        while (ctx.hasScheduled())
            scheduled.add(ctx.getNextScheduled());

        Assert.assertEquals(expected, scheduled);
        ctx.clearFromRunning(SlotKey.parse("metrics_5m,0,0")); //metrics_5m,0 is now rolled up.

    }


    @Test
    public void testSimpleUpdateAndScheduleWithLongerDelayedMetrics() {
        //testing for one slot -> 0

        final long initialCollectionTime = 1209600000000L;
        //***************************************************************************************
        //*  Testing for on-time metrics (ROLLUP_DELAY_MILLIS)                                  *
        //***************************************************************************************

        long clock = initialCollectionTime;
        final Clock mockClock = Mockito.mock(Clock.class);

        ScheduleContext ctx = new ScheduleContext(clock, shards, mockClock);
        Collection<SlotKey> scheduled = new ArrayList<SlotKey>();
        Collection<SlotKey> expected = new ArrayList<SlotKey>();

        ctx.setCurrentTimeMillis(clock); // +0m
        when(mockClock.now()).thenReturn(new Instant(initialCollectionTime + 1));
        ctx.update(clock, shards.get(0)); //metric ingestion

        clock = initialCollectionTime + ROLLUP_DELAY_MILLIS + 1; // a little bit over +5m
        ctx.setCurrentTimeMillis(clock);
        ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);

        expected.add(SlotKey.parse("metrics_5m,0,0"));
        while (ctx.hasScheduled())
            scheduled.add(ctx.getNextScheduled());
        Assert.assertEquals(expected, scheduled);

        long lastRollupTime = clock;
        when(mockClock.now()).thenReturn(new Instant(lastRollupTime));
        ctx.clearFromRunning(SlotKey.parse("metrics_5m,0,0")); //metrics_5m,0 is now rolled up.
        ctx.clearFromRunning(SlotKey.parse("metrics_20m,0,0"));
        ctx.clearFromRunning(SlotKey.parse("metrics_60m,0,0"));
        ctx.clearFromRunning(SlotKey.parse("metrics_240m,0,0"));
        ctx.clearFromRunning(SlotKey.parse("metrics_1440m,0,0"));

        scheduled.clear();

        //****************************************************************************************
        //*  Testing for long delay metrics(LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS)                           *
        //****************************************************************************************

        //delayed metrics with longer delay for slot 0 (metrics sent after SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS)
        long lastIngestTime2 = initialCollectionTime + SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS + 300000;
        when(mockClock.now()).thenReturn(new Instant(lastIngestTime2));
        long collectionTime2 = initialCollectionTime + 2;
        ctx.update(collectionTime2, shards.get(0)); //metric ingestion
        ctx.setCurrentTimeMillis(lastIngestTime2 + 1);
        ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);

        Assert.assertFalse("Rollup should not be scheduled for re-roll cos of rollup_wait", ctx.hasScheduled());

        //Trying for rollup again after SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS past since last collection time
        clock = lastIngestTime2 + LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS + 1;
        ctx.setCurrentTimeMillis(clock);
        ctx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);

        while (ctx.hasScheduled())
            scheduled.add(ctx.getNextScheduled());

        Assert.assertEquals(expected, scheduled);
        ctx.clearFromRunning(SlotKey.parse("metrics_5m,0,0")); //metrics_5m,0 is now rolled up.

    }
}
