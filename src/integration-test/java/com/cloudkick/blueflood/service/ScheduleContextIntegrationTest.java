package com.cloudkick.blueflood.service;

import com.cloudkick.blueflood.rollup.Granularity;
import com.google.common.collect.Sets;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ScheduleContextIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger("tests");
    private ScheduleContext context;
    private Collection<Integer> manageShards = new HashSet<Integer>();

    static {
        try {
            Configuration.init();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Before
    public void setUp() {
        manageShards.add(1);
        manageShards.add(5);
        manageShards.add(7);
        manageShards.add(11);
        context = new ScheduleContext(1234, manageShards);
    }

    @Test
    public void testSetShardAddition() throws Exception {
        Assert.assertArrayEquals(manageShards.toArray(), context.getManagedShards().toArray());
        manageShards.add(2);
        context.addShard(2);
        Assert.assertArrayEquals(manageShards.toArray(), context.getManagedShards().toArray());
        final ZKBasedShardLockManager lockManager = (ZKBasedShardLockManager) Whitebox.getInternalState(context,
                "lockManager");

        Map<Integer, InterProcessMutex> lockObjects = (Map<Integer, InterProcessMutex>) Whitebox.getInternalState
                (lockManager, "locks");

        Assert.assertTrue(lockObjects.get(2) != null);  // assert that we have a lock object for shard "2"
    }

    @Test
    public void testSetShardDeletion() {
        Assert.assertArrayEquals(manageShards.toArray(), context.getManagedShards().toArray());
        manageShards.remove(1);
        context.removeShard(1);
        Assert.assertArrayEquals(manageShards.toArray(), context.getManagedShards().toArray());
        final ZKBasedShardLockManager lockManager = (ZKBasedShardLockManager) Whitebox.getInternalState(context,
                "lockManager");

        Map<Integer, InterProcessMutex> lockObjects = (Map<Integer, InterProcessMutex>) Whitebox.getInternalState
                (lockManager, "locks");

        Assert.assertTrue(lockObjects.get(1) == null);  // assert that we don't have a lock object for shard "1"
    }
}