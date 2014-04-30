package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.SlotState;
import com.rackspacecloud.blueflood.service.UpdateStamp;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

// todo: a more consistent API would be good. The current API was copied verbatim from the AstReader and AstWriter classes.
public interface ShardStateIO {
    public Collection<SlotState> getShardState(int shard) throws IOException;
    public void putShardState(int shard, Map<Granularity, Map<Integer, UpdateStamp>> slotTimes) throws IOException;
}
