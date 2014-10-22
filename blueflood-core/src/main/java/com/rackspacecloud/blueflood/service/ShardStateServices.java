package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.io.ShardStateIO;
import com.rackspacecloud.blueflood.utils.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;

/**
 * Aggregates the ShardStateServices.
 */
public class ShardStateServices {
    private static final Logger log = LoggerFactory.getLogger(ShardStateServices.class);

    private final ScheduleContext context;
    private final ShardStatePusher pusher;
    private final ShardStatePuller puller;

    public ShardStateServices(ScheduleContext context, ShardStateIO io) {
        this.context = context;

        // these threads are responsible for sending/receiving schedule context state to/from the database.
        final Collection<Integer> allShards = Collections.unmodifiableCollection(Util.parseShards("ALL"));
        pusher = new ShardStatePusher(allShards, context.getShardStateManager(), io);
        puller = new ShardStatePuller(allShards, context.getShardStateManager(), io);

        pusher.setActive(false);
        puller.setActive(false);

       Configuration config = Configuration.getInstance();
        if (config.getBooleanProperty(CoreConfig.INGEST_MODE) || config.getBooleanProperty(CoreConfig.ROLLUP_MODE)) {
            try {
                final Thread shardPush = new Thread(pusher, "Shard state writer");
                final Thread shardPull = new Thread(puller, "Shard state reader");

                shardPull.start();
                shardPush.start();

                log.info("Shard push and pull services started");
            } catch (NumberFormatException ex) {
                log.error("Shard services not started. Probably misconfiguration", ex);
            }
        } else {
            log.info("Shard push and pull services not required");
        }

        // if things were enabled, threads are actually running at this point, but are blocked until enabled.
    }

    public void start() {
        pusher.setActive(true);
        puller.setActive(true);
    }

    public void stop() {
        pusher.setActive(false);
        puller.setActive(false);
    }
}
