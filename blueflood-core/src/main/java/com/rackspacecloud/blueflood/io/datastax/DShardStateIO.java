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
public class DShardStateIO implements ShardStateIO {

    public static final String KEY = "key";
    public static final String COLUMN1 = "column1";
    public static final String VALUE = "value";
    public static final String WRITE_TIME = "writetime(value)";

    private static final Logger LOG = LoggerFactory.getLogger(DShardStateIO.class);

    private final SlotStateSerDes serDes = new SlotStateSerDes();

    private PreparedStatement getShardState;
    private PreparedStatement putShardState;

    public DShardStateIO() {

        createPreparedStatements();
    }

    private void createPreparedStatements() {

        Select.Where statement = select()
                .column( KEY )
                .column( COLUMN1 )
                .column( VALUE )
                .writeTime( VALUE )
                .from( CassandraModel.CF_METRICS_STATE_NAME)
                .where(eq(KEY, bindMarker() ));

        getShardState = DatastaxIO.getSession().prepare( statement );

        Insert insert = insertInto(CassandraModel.CF_METRICS_STATE_NAME)
                .value( KEY, bindMarker() )
                .value( COLUMN1, bindMarker() )
                .value( VALUE, bindMarker() );

        putShardState = DatastaxIO.getSession().prepare( insert );
        // TODO: This is required by the cassandra-maven-plugin 2.0.0-1, but not by cassandra 2.0.11, which we run.
        // I believe its due to the bug https://issues.apache.org/jira/browse/CASSANDRA-6238
        putShardState.setConsistencyLevel( ConsistencyLevel.ONE );
    }

    @Override
    public Collection<SlotState> getShardState(int shard) throws IOException {

        Timer.Context ctx = Instrumentation.getReadTimerContext(CassandraModel.CF_METRICS_STATE_NAME);
        final Collection<SlotState> slotStates = new LinkedList<SlotState>();
        Session session = DatastaxIO.getSession();

        try {

            BoundStatement bound = getShardState.bind((long) shard);

            List<Row> results = session.execute(bound).all();

            for (Row row : results) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace(String.format("Read shard: %d: - %s %s\n",
                            row.getLong( KEY ),
                            row.getString( COLUMN1 ),
                            row.getLong( VALUE ),
                            row.getLong( WRITE_TIME )));
                }
                SlotState state = serDes.deserialize(row.getString( COLUMN1 ));
                state.withTimestamp(row.getLong( VALUE ))
                     .withLastUpdatedTimestamp(row.getLong( WRITE_TIME ) / 1000); //write time is in micro seconds
                slotStates.add(state);
            }
            return slotStates;
        } finally {
            ctx.stop();
        }
    }

    @Override
    public void putShardState(int shard, Map<Granularity, Map<Integer, UpdateStamp>> slotTimes) throws IOException {

        Timer.Context ctx = Instrumentation.getWriteTimerContext(CassandraModel.CF_METRICS_STATE_NAME);
        Session session = DatastaxIO.getSession();

        Map<String, ResultSetFuture> futures = new HashMap<String, ResultSetFuture>();

        try {
            Set<Granularity> granularities = slotTimes.keySet();
            for (Granularity gran : granularities) {

                Set<Map.Entry<Integer, UpdateStamp>> entries = slotTimes.get(gran).entrySet();
                for (Map.Entry<Integer, UpdateStamp> entry : entries) {

                    String value = serDes.serialize(gran, entry.getKey(), entry.getValue().getState());

                    BoundStatement bound = putShardState.bind( (long) shard,
                            value,
                            entry.getValue().getTimestamp());

                    futures.put( "shard: " + shard +", " + value, session.executeAsync( bound ) );
                }
            }

            for( Map.Entry<String, ResultSetFuture> future : futures.entrySet() ) {

                try {
                    ResultSet result = future.getValue().getUninterruptibly();
                    LOG.trace( "results.size=" + result.all().size() );
                }
                catch ( Exception e ) {

                    LOG.error( String.format( "error writing to metrics_state: %s", future.getKey() ), e );
                }
            }

        } finally {
            ctx.stop();
        }
    }

}
