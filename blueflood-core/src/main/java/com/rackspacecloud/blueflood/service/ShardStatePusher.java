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

import com.codahale.metrics.Timer;
import com.rackspacecloud.blueflood.io.AstyanaxShardStateIO;
import com.rackspacecloud.blueflood.io.ShardStateIO;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ShardStatePusher extends ShardStateWorker {
    private static final Logger log = LoggerFactory.getLogger(ShardStatePusher.class);
    
    public ShardStatePusher(final Collection<Integer> allShards, ShardStateManager stateManager, ShardStateIO io) {
        super(allShards, stateManager, new TimeValue(Configuration.getInstance().getIntegerProperty(CoreConfig.SHARD_PUSH_PERIOD), TimeUnit.MILLISECONDS), io);
    }

    public void performOperation() {
        Timer.Context ctx = timer.time();
        try {
            for (int shard : allShards) {
                Map<Granularity, Map<Integer, UpdateStamp>> slotTimes = shardStateManager.getDirtySlotsToPersist(shard);
                if (slotTimes != null) {
                    try {
                        getIO().putShardState(shard, slotTimes);
                    } catch (IOException ex) {
                        log.error("Could not put shard state to the database (shard " + shard + "). " + ex.getMessage(), ex);
                    }
                }
            }
        } catch (RuntimeException ex) {
            log.error("Could not put shard states to the database. " + ex.getMessage(), ex);
        } finally {
            ctx.stop();
        }
    }
    
}
