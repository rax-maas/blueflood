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
package com.rackspacecloud.blueflood.io.datastax;

import com.codahale.metrics.Timer;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.*;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.Instrumentation;
import com.rackspacecloud.blueflood.io.ShardStateIO;
import com.rackspacecloud.blueflood.io.serializers.metrics.SlotStateSerDes;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.SlotState;
import com.rackspacecloud.blueflood.service.UpdateStamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

/**
 * This class uses the Datastax driver to read/write ShardState from
 * Cassandra metrics_state Column Family.
 */
public class DatastaxShardStateIO implements ShardStateIO {

    private static final Logger LOG = LoggerFactory.getLogger(DatastaxShardStateIO.class);

    private final SlotStateSerDes serDes = new SlotStateSerDes();

    @Override
    public Collection<SlotState> getShardState(int shard) throws IOException {

        Timer.Context ctx = Instrumentation.getReadTimerContext(CassandraModel.CF_METRICS_STATE_NAME);
        final Collection<SlotState> slotStates = new LinkedList<SlotState>();
        Session session = DatastaxIO.getSession();

        try {
            Select.Where statement = QueryBuilder
                    .select()
                    .all()
                    .from(CassandraModel.QUOTED_KEYSPACE, CassandraModel.CF_METRICS_STATE_NAME)
                    .where(eq("key", (long) shard));
            List<Row> results = session.execute(statement).all();
            for (Row row : results) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Read shard: %d: - %s %s\n",
                            row.getLong("key"),
                            row.getString("column1"),
                            row.getLong("value")));
                }
                SlotState state = serDes.deserialize(row.getString("column1"));
                state.withTimestamp(row.getLong("value"));
                slotStates.add(state);
            }
            return slotStates;
        } finally {
            ctx.stop();
        }
    }

    @Override
    public void putShardState(int shard, Map<Granularity, Map<Integer, UpdateStamp>> slotTimes) throws IOException {

        Timer.Context ctx = Instrumentation.getReadTimerContext(CassandraModel.CF_METRICS_STATE_NAME);
        Session session = DatastaxIO.getSession();
        Batch batch = QueryBuilder.batch();

        try {
            Set<Granularity> granularities = slotTimes.keySet();
            for (Granularity gran : granularities) {

                Set<Map.Entry<Integer, UpdateStamp>> entries = slotTimes.get(gran).entrySet();
                for (Map.Entry<Integer, UpdateStamp> entry : entries) {
                    Insert insert = QueryBuilder.insertInto(
                            CassandraModel.QUOTED_KEYSPACE, CassandraModel.CF_METRICS_STATE_NAME)
                            .value("key", shard)
                            .value("column1", serDes.serialize(gran, entry.getKey(), entry.getValue().getState()))
                            .value("value", entry.getValue().getTimestamp());
                    batch.add(insert);
                }
            }
            ResultSet results = session.execute(batch);
            LOG.debug("results.size=" + results.all().size());
        } finally {
            ctx.stop();
        }
    }

}
