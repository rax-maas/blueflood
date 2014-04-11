package com.rackspacecloud.blueflood.dw.ingest;

import com.rackspacecloud.blueflood.service.ScheduleContext;

import java.util.HashMap;
import java.util.Map;

public class ShardUpdates {
    private Map<Integer, Long> updates = new HashMap<Integer, Long>();
    
    public void update(long when, int shard) {
        Long current = updates.get(shard);
        if (current == null) {
            updates.put(shard, when);
        } else {
            if (when > current) {
                updates.put(shard, when);
            }
        }
    }
    
    public void flush(ScheduleContext context) {
        for (Map.Entry<Integer, Long> entry : updates.entrySet()) {
            context.update(entry.getValue(), entry.getKey());
        }
    }
}
