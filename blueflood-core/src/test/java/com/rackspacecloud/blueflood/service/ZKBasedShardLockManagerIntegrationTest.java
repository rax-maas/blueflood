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

import com.rackspacecloud.blueflood.utils.ZookeeperTestServer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ZKBasedShardLockManagerIntegrationTest {
    private Set<Integer> manageShards = null;
    private ZKBasedShardLockManager lockManager;
    private ZookeeperTestServer zkTestServer;

    @Before
    public void setUp() throws Exception {
        zkTestServer = new ZookeeperTestServer();
        zkTestServer.connect();
        manageShards = new HashSet<Integer>();
        manageShards.add(1);
        lockManager = new ZKBasedShardLockManager(zkTestServer.getZkConnect(), manageShards);
        Assert.assertTrue("Zookeeper connection is needed.", lockManager.waitForZKConnections(10));
        lockManager.prefetchLocks();
    }

    @After
    public void tearDown() throws Exception {
        lockManager.shutdownUnsafe();
        zkTestServer.shutdown();
    }

    @Test
    public void testAddShard() throws Exception {
        final int shard = 20;

        // internal lock object should not be present.
        Assert.assertNull(lockManager.getLockUnsafe(shard));

        // after adding, it should be present.
        lockManager.addShard(shard);
        Assert.assertNotNull(lockManager.getLockUnsafe(shard));  // assert that we have a lock object for shard "20"

        // but we cannot do work until the lock is acquired.
        Assert.assertFalse(lockManager.canWork(shard));

        // let the lock be acquired.
        lockManager.forceLockScavenge(); // lock will attempt.
        lockManager.waitForQuiesceUnsafe();

        // verify can work and lock is held.
        Assert.assertTrue(lockManager.canWork(shard));
        Assert.assertTrue(lockManager.holdsLockUnsafe(shard));

        lockManager.releaseLockUnsafe(shard);
    }

    @Test
    public void testRemoveShard() {
        final int shard = 1;
        Assert.assertTrue(lockManager.getLockUnsafe(shard) != null);  // assert that we have a lock object for shard "20"
        Assert.assertTrue(lockManager.canWork(shard));
        Assert.assertTrue(lockManager.holdsLockUnsafe(shard));

        // remove the shard, should also remove the lock.
        lockManager.removeShard(1);
        lockManager.waitForQuiesceUnsafe();

        Assert.assertFalse(lockManager.holdsLockUnsafe(shard));
        Assert.assertFalse(lockManager.canWork(shard));

        Assert.assertNull(lockManager.getLockUnsafe(shard)); // assert that we don't have a lock object for shard "1"
    }

    @Test
    public void testHappyCaseLockAcquireAndRelease() throws Exception {
        final Integer shard = 1;
        Assert.assertTrue(lockManager.canWork(shard));

        // Check if lock is acquired
        Assert.assertTrue(lockManager.holdsLockUnsafe(shard));
        Assert.assertTrue(lockManager.releaseLockUnsafe(shard));

        // Check we don't hold the lock
        Assert.assertFalse(lockManager.canWork(shard));
        Assert.assertFalse(lockManager.holdsLockUnsafe(shard));

        lockManager.releaseLockUnsafe(shard);
    }

    @Test
    public void testZKConnectionLoss() throws Exception {
        final Integer shard = 1;
        Assert.assertTrue(lockManager.canWork(shard));
        lockManager.waitForQuiesceUnsafe();

        // Check if lock is acquired
        Assert.assertTrue(lockManager.holdsLockUnsafe(shard));

        // simulate connection loss
        lockManager.stateChanged((CuratorFramework)Whitebox.getInternalState(lockManager, "client"), ConnectionState.LOST);

        // Check we don't hold the lock, but we should still be able to work.
        Assert.assertFalse(lockManager.holdsLockUnsafe(shard)); // lock is technically lost.

        // Check that no locks are held
        Collection<Integer> heldLocks = lockManager.getHeldShards();
        Assert.assertTrue(heldLocks.isEmpty());

        // Check all locks are in state LockState.ERROR
        Map<Integer, ZKBasedShardLockManager.Lock> locks = (Map<Integer, ZKBasedShardLockManager.Lock>) Whitebox.getInternalState(lockManager, "locks");
        for (Map.Entry<Integer, ZKBasedShardLockManager.Lock> lockEntry : locks.entrySet()) {
            Assert.assertTrue(lockEntry.getValue().getLockState() == ZKBasedShardLockManager.LockState.ERROR);
        }

        // but we can still do work.
        for (Map.Entry<Integer, ZKBasedShardLockManager.Lock> lockEntry : locks.entrySet()) {
            Assert.assertTrue(lockManager.canWork(shard));
        }

        // Simulate connection re-establishment
        lockManager.stateChanged((CuratorFramework)Whitebox.getInternalState(lockManager, "client"), ConnectionState.RECONNECTED);

        // Force lock scavenge
        lockManager.forceLockScavenge();

        // Check all locks state. They could be UNKNOWN or ACQUIRED (ultra-fast ZK).
        for (Map.Entry<Integer, ZKBasedShardLockManager.Lock> lockEntry : locks.entrySet()) {
            Assert.assertTrue(lockEntry.getValue().getLockState() == ZKBasedShardLockManager.LockState.UNKNOWN
                    || lockEntry.getValue().getLockState() == ZKBasedShardLockManager.LockState.ACQUIRED);
        }

        lockManager.releaseLockUnsafe(shard);
    }

    @Test
    public void testDuelingManagers() throws Exception {
        final int shard = 1;
        ZKBasedShardLockManager otherManager = new ZKBasedShardLockManager(zkTestServer.getZkConnect(), manageShards);
        Assert.assertTrue("Zookeeper connection is needed.", otherManager.waitForZKConnections(10));
        otherManager.prefetchLocks();
        otherManager.waitForQuiesceUnsafe();

        // first manager.
        Assert.assertTrue(lockManager.canWork(shard));
        lockManager.waitForQuiesceUnsafe();
        Assert.assertTrue(lockManager.holdsLockUnsafe(shard));

        // second manager could not acquire lock.
        Assert.assertFalse(otherManager.canWork(shard));
        Assert.assertFalse(otherManager.holdsLockUnsafe(shard));
        Assert.assertFalse(otherManager.canWork(shard));

        // force first manager to give up the lock.
        lockManager.setMinLockHoldTimeMillis(0);
        lockManager.setLockDisinterestedTimeMillis(300000);
        lockManager.forceLockScavenge();
        lockManager.waitForQuiesceUnsafe();
        Assert.assertFalse(lockManager.canWork(shard));
        Assert.assertFalse(lockManager.holdsLockUnsafe(shard));

        // see if second manager picks it up.
        otherManager.setLockDisinterestedTimeMillis(0);
        otherManager.forceLockScavenge();
        otherManager.waitForQuiesceUnsafe();
        Assert.assertTrue(otherManager.canWork(shard));
        Assert.assertTrue(otherManager.holdsLockUnsafe(shard));

        otherManager.shutdownUnsafe();
    }

    @Test
    public void testConviction() throws Exception {
        for (int shard : manageShards) {
            Assert.assertTrue(lockManager.canWork(shard));
            Assert.assertTrue(lockManager.holdsLockUnsafe(shard));
        }

        // force locks to be dropped.
        lockManager.setMinLockHoldTimeMillis(0);
        lockManager.forceLockScavenge();
        lockManager.waitForQuiesceUnsafe();

        // should not be able to work.
        for (int shard : manageShards) {
            Assert.assertFalse(lockManager.holdsLockUnsafe(shard));
            Assert.assertFalse(lockManager.canWork(shard));
        }

        // see if locks are picked back up.
        lockManager.setMinLockHoldTimeMillis(10000);
        lockManager.setLockDisinterestedTimeMillis(0);
        lockManager.forceLockScavenge();
        lockManager.waitForQuiesceUnsafe();
        for (int shard : manageShards) {
            Assert.assertTrue(lockManager.canWork(shard));
            Assert.assertTrue(lockManager.holdsLockUnsafe(shard));
        }
    }
}
