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
 * Created by shin4590 on 3/25/16.
 */
public class DatastaxShardStateIO implements ShardStateIO {

    private static final Logger LOG = LoggerFactory.getLogger(DatastaxShardStateIO.class);
    private static final SlotStateSerDes serDes = new SlotStateSerDes();

    @Override
    public Collection<SlotState> getShardState(int shard) throws IOException {

        Timer.Context ctx = Instrumentation.getReadTimerContext(CassandraModel.CF_METRICS_STATE_NAME);
        final Collection<SlotState> slotStates = new LinkedList<SlotState>();
        Session session = DatastaxIO.getSession();

        try {
            Select.Where statement = QueryBuilder
                    .select()
                    .all()
                    .from("\"DATA\"", CassandraModel.CF_METRICS_STATE_NAME)
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
            LOG.debug("results.size" + results.all().size());
        } finally {
            ctx.stop();
        }
    }

}
