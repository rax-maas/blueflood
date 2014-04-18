package com.rackspacecloud.blueflood.dw.ingest;

import com.rackspacecloud.blueflood.service.ScheduleContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Reduce contention on the locks in a ScheduleContext object by keeping all of its updates until they can all be
 * flushed at once.
 * 
 * This class is NOT threadsafe.
 */
public class ShardUpdates {
    private Map<Integer, Long> updates = new HashMap<Integer, Long>();
    
    public void update(long when, int shard) {
        updates.put(shard, when);
    }
    
    public void flush(ScheduleContext context) {
        for (Map.Entry<Integer, Long> entry : updates.entrySet()) {
            context.update(entry.getValue(), entry.getKey());
        }
    }
}
