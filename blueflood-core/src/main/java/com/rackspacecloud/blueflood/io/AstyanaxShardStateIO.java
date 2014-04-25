package com.rackspacecloud.blueflood.io;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.SlotState;
import com.rackspacecloud.blueflood.service.UpdateStamp;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class AstyanaxShardStateIO implements ShardStateIO {
    private AstyanaxReader reader = AstyanaxReader.getInstance();
    private AstyanaxWriter writer = AstyanaxWriter.getInstance();
    
    @Override
    public Collection<SlotState> getShardState(int shard) throws IOException {
        try {
            return reader.getShardState(shard);
        } catch (RuntimeException ex) {
            throw new IOException(ex.getCause());
        }
    }

    @Override
    public void putShardState(int shard, Map<Granularity, Map<Integer, UpdateStamp>> slotTimes) throws IOException {
        try {
            writer.persistShardState(shard, slotTimes);
        } catch (ConnectionException ex) {
            throw new IOException(ex);
        }
    }
}
