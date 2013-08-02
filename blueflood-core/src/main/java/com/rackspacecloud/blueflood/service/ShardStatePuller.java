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

    ShardStatePuller(Collection<Integer> allShards, ShardStateManager stateManager) {
        super(allShards, stateManager, new TimeValue(Configuration.getIntegerProperty("SHARD_PULL_PERIOD"), TimeUnit.MILLISECONDS));
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
