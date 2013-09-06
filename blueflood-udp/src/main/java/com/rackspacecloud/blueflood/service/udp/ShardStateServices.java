package com.rackspacecloud.blueflood.service.udp;

import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.ScheduleContext;
import com.rackspacecloud.blueflood.service.ShardStatePuller;
import com.rackspacecloud.blueflood.service.ShardStatePusher;
import com.rackspacecloud.blueflood.utils.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;

/**
 * todo: There is currently no way to shut the threads down once they start.  This will need to be done by modifying
 * ShardStateWorker to support start/stop.
 */
public class ShardStateServices {
    private static final Logger log = LoggerFactory.getLogger(ShardStateServices.class);
    
    private final ScheduleContext context;
    
    public ShardStateServices(ScheduleContext context) {
        this.context = context;
    }
    
    public void start() {
        if (Configuration.getBooleanProperty("INGEST_MODE") || Configuration.getBooleanProperty("ROLLUP_MODE")) {
            
            // these threads are responsible for sending/receiving schedule context state to/from the database.
            final Collection<Integer> allShards = Collections.unmodifiableCollection(Util.parseShards("ALL"));
            
            try {
                final Thread shardPush = new Thread(new ShardStatePusher(allShards, context.getShardStateManager()), "Shard state writer");
                final Thread shardPull = new Thread(new ShardStatePuller(allShards, context.getShardStateManager()), "Shard state reader");
                
                shardPull.start();
                shardPush.start();
                
                log.info("Shard push and pull services started");
            } catch (NumberFormatException ex) {
                log.error("Shard services not started. Probably misconfiguration", ex);
            }
        } else {
            log.info("Shard push and pull services not required");
        }
    }
}
