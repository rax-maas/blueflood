package com.cloudkick.blueflood.service;

import com.cloudkick.blueflood.io.AstyanaxReader;
import com.cloudkick.blueflood.rollup.Granularity;
import com.cloudkick.blueflood.utils.TimeValue;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.yammer.metrics.core.TimerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ShardStatePuller extends ShardStateWorker {
    private static final Logger log = LoggerFactory.getLogger(ShardStatePuller.class);

    ShardStatePuller(Collection<Integer> allShards, ScheduleContext context) {
        super(allShards, context, new TimeValue(Configuration.getIntegerProperty("SHARD_PULL_PERIOD"), TimeUnit.MILLISECONDS));
    }

    public void performOperation() {
        TimerContext ctx = timer.time();
        try {
            AstyanaxReader reader = AstyanaxReader.getInstance();
            Map<Integer, Map<Granularity, Map<Integer, UpdateStamp>>> updates = reader.getAllShardStates(context.getManagedShards());
            for (Map.Entry<Integer, Map<Granularity, Map<Integer, UpdateStamp>>> shardUpdates : updates.entrySet())
                for (Map.Entry<Granularity, Map<Integer, UpdateStamp>> granUpdates : shardUpdates.getValue().entrySet())
                    context.applySlotStamps(granUpdates.getKey(), shardUpdates.getKey(), granUpdates.getValue());
        } catch (ConnectionException ex) {
            log.error("Could not read shard state from the database. " + ex.getMessage(), ex);
        } finally {
            ctx.stop();
        }
    }
}
