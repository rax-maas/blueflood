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

import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.utils.TimeValue;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.yammer.metrics.core.TimerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class ShardStatePuller extends ShardStateWorker {
    private static final Logger log = LoggerFactory.getLogger(ShardStatePuller.class);

    public ShardStatePuller(Collection<Integer> allShards, ShardStateManager stateManager) {
        super(allShards, stateManager, new TimeValue(Configuration.getInstance().getIntegerProperty(CoreConfig.SHARD_PULL_PERIOD), TimeUnit.MILLISECONDS));
    }

    public void performOperation() {
        TimerContext ctx = timer.time();
        try {
            AstyanaxReader reader = AstyanaxReader.getInstance();
            reader.getAndUpdateAllShardStates(shardStateManager, shardStateManager.getManagedShards());
        } catch (ConnectionException ex) {
            log.error("Could not read shard state from the database. " + ex.getMessage(), ex);
        } finally {
            ctx.stop();
        }
    }
}
