package com.cloudkick.blueflood.service;

import com.cloudkick.blueflood.io.AstyanaxWriter;
import com.cloudkick.blueflood.rollup.Granularity;
import com.cloudkick.blueflood.utils.TimeValue;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.yammer.metrics.core.TimerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class ShardStatePusher extends ShardStateWorker {
    private static final Logger log = LoggerFactory.getLogger(ShardStatePusher.class);

    ShardStatePusher(final Collection<Integer> allShards, ScheduleContext context) {
        super(allShards, context, new TimeValue(Configuration.getIntegerProperty("SHARD_PUSH_PERIOD"), TimeUnit.MILLISECONDS));
    }
    
    public void performOperation() {
        long start = System.currentTimeMillis();
        TimerContext ctx = timer.time();
        try {
            AstyanaxWriter writer = AstyanaxWriter.getInstance();
            for (int shard : allShards) {
                // only push if this shard has been updated since it was last pushed.
                int numUpdates = 0;
                // granularity->slot->UpdateStamp
                Map<Granularity, Map<Integer, UpdateStamp>> slotTimes = new HashMap<Granularity, Map<Integer, UpdateStamp>>();
                for (Granularity gran : Granularity.values()) {
                    if (gran == Granularity.FULL) continue;
                    // slot->UpdateStamp
                    Map<Integer, UpdateStamp> dirty = context.getDirtySlotStamps(gran, shard);
                    slotTimes.put(gran, dirty);
                    // mark clean here.
                    for (UpdateStamp stamp : dirty.values())
                        stamp.setDirty(false);
                    numUpdates += dirty.size();
                }
                if (numUpdates > 0) {
                    writer.persistShardState(shard, slotTimes);
                    // for updates that come by way of scribe, you'll typically see 5 as the number of updates (one for
                    // each granularity).  On rollup slaves the situation is a bit different. You'll see only the slot
                    // of the granularity just written to marked dirty (so 1).
                    log.debug("Pushed {} from shard {} in {}", new Object[]{numUpdates, shard, System.currentTimeMillis() - start});
                }
            }       
        } catch (RuntimeException ex) {
            log.error("Could not write shard state to the database. " + ex.getMessage(), ex);
        } catch (ConnectionException ex) {
            log.error("Could not write shard state to the database. " + ex.getMessage(), ex);
        } finally {
            ctx.stop();
        }
    }
}
