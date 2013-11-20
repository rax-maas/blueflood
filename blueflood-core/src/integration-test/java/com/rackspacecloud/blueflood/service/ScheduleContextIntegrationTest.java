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

import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

public class ScheduleContextIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger("tests");
    private ScheduleContext context;
    private ShardStateManager shardStateManager;
    private Collection<Integer> manageShards = new HashSet<Integer>();

    @Before
    public void setUp() {
        manageShards.add(1);
        manageShards.add(5);
        manageShards.add(7);
        manageShards.add(11);
        context = new ScheduleContext(1234, manageShards, Configuration.getInstance().getStringProperty(CoreConfig.ZOOKEEPER_CLUSTER));
        shardStateManager = context.getShardStateManager();
    }

    @Test
    public void testSetShardAddition() throws Exception {
        Assert.assertArrayEquals(manageShards.toArray(), shardStateManager.getManagedShards().toArray());
        manageShards.add(2);
        context.addShard(2);
        Assert.assertArrayEquals(manageShards.toArray(), shardStateManager.getManagedShards().toArray());
        final ZKBasedShardLockManager lockManager = (ZKBasedShardLockManager) Whitebox.getInternalState(context,
                "lockManager");

        Map<Integer, InterProcessMutex> lockObjects = (Map<Integer, InterProcessMutex>) Whitebox.getInternalState
                (lockManager, "locks");

        Assert.assertTrue(lockObjects.get(2) != null);  // assert that we have a lock object for shard "2"
    }

    @Test
    public void testSetShardDeletion() {
        Assert.assertArrayEquals(manageShards.toArray(), shardStateManager.getManagedShards().toArray());
        manageShards.remove(1);
        context.removeShard(1);
        Assert.assertArrayEquals(manageShards.toArray(), shardStateManager.getManagedShards().toArray());
        final ZKBasedShardLockManager lockManager = (ZKBasedShardLockManager) Whitebox.getInternalState(context,
                "lockManager");

        Map<Integer, InterProcessMutex> lockObjects = (Map<Integer, InterProcessMutex>) Whitebox.getInternalState
                (lockManager, "locks");

        Assert.assertTrue(lockObjects.get(1) == null);  // assert that we don't have a lock object for shard "1"
    }
}