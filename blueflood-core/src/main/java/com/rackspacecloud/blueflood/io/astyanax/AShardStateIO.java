/*
 * Copyright (c) 2016 Rackspace.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.rackspacecloud.blueflood.io.astyanax;

import com.codahale.metrics.Timer;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.SlotState;
import com.rackspacecloud.blueflood.service.UpdateStamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

/**
 * This class uses the Astyanax driver to read/write ShardState from
 * Cassandra metrics_state Column Family.
 */
public class AShardStateIO implements ShardStateIO {

    private static final Logger LOG = LoggerFactory.getLogger(AShardStateIO.class);

    @Override
    public Collection<SlotState> getShardState(int shard) throws IOException {
        AstyanaxIO astyanaxIO = AstyanaxIO.singleton();
        Timer.Context ctx = Instrumentation.getReadTimerContext(CassandraModel.CF_METRICS_STATE_NAME);
        final Collection<SlotState> slotStates = new LinkedList<SlotState>();
        try {
            ColumnList<SlotState> columns = astyanaxIO.getKeyspace().prepareQuery(CassandraModel.CF_METRICS_STATE)
                    .getKey((long)shard)
                    .execute()
                    .getResult();

            for (Column<SlotState> column : columns) {
                slotStates.add(column.getName()
                                 .withTimestamp(column.getLongValue())
                                 .withLastUpdatedTimestamp(column.getTimestamp() / 1000)); //write time is in micro seconds
            }
        } catch (ConnectionException e) {
            Instrumentation.markReadError(e);
            LOG.error("Error getting shard state for shard " + shard, e);
            throw new IOException(e);
        } finally {
            ctx.stop();
        }
        return slotStates;
    }

    @Override
    public void putShardState(int shard, Map<Granularity, Map<Integer, UpdateStamp>> slotTimes) throws IOException {
        AstyanaxIO astyanaxIO = AstyanaxIO.singleton();
        Timer.Context ctx = Instrumentation.getWriteTimerContext(CassandraModel.CF_METRICS_STATE_NAME);
        try {
            MutationBatch mutationBatch = astyanaxIO.getKeyspace().prepareMutationBatch();
            ColumnListMutation<SlotState> mutation = mutationBatch.withRow(CassandraModel.CF_METRICS_STATE, (long)shard);
            for (Map.Entry<Granularity, Map<Integer, UpdateStamp>> granEntry : slotTimes.entrySet()) {
                Granularity g = granEntry.getKey();
                for (Map.Entry<Integer, UpdateStamp> entry : granEntry.getValue().entrySet()) {
                    // granularity,slot,state
                    SlotState slotState = new SlotState(g, entry.getKey(), entry.getValue().getState());
                    mutation.putColumn(slotState, entry.getValue().getTimestamp());
                    /*
                      Note: this method used to set the timestamp of the Cassandra column to entry.getValue().getTimestamp() * 1000, i.e. the collection time.
                      That implementation was changed because it could cause delayed metrics not to rollup.
                      Consider you are getting out of order metrics M1 and M2, with collection times T1 and T2 with T2>T1, belonging to same slot
                      Assume M2 arrives first. The slot gets marked active and rolled up and the state is set as Rolled. Now, assume M1 arrives. We update the slot state to active,
                      set the slot timestamp to T1, and while persisting we set it, we set the column timestamp to be T1*1000, but because the T1 < T2, Cassandra wasn't updating it.
                     */
                }
            }
            if (!mutationBatch.isEmpty())
                try {
                    mutationBatch.execute();
                } catch (ConnectionException e) {
                    Instrumentation.markWriteError(e);
                    LOG.error("Error persisting shard state", e);
                    throw new IOException(e);
                }
        } finally {
            ctx.stop();
        }
    }
}
