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
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.ExcessEnumIO;
import com.rackspacecloud.blueflood.io.Instrumentation;
import com.rackspacecloud.blueflood.types.Locator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;

/**
 * This class uses the Datastax driver to read/write excess enum metrics from
 * Cassandra metrics_excess_enums Column Family.
 */
public class DExcessEnumIO implements ExcessEnumIO {

    private static final Logger LOG = LoggerFactory.getLogger(DExcessEnumIO.class);

    private static final String KEY = "key";
    private static final String COLUMN1 = "column1";
    private static final String VALUE = "value";

    private PreparedStatement getValue;
    private PreparedStatement putValue;

    public DExcessEnumIO() {
        createPreparedStatements();
    }

    /**
     * Create all prepared statements use in this class for metrics_excess_enums
     */
    private void createPreparedStatements()  {

        // create a generic select statement for retrieving from metrics_locator
        Select select = QueryBuilder
                .select()
                .all()
                .from(CassandraModel.CF_METRICS_EXCESS_ENUMS_NAME);
        getValue = DatastaxIO.getSession().prepare( select );

        // create a generic insert statement for inserting into metrics_locator
        Insert insert = QueryBuilder.insertInto( CassandraModel.CF_METRICS_EXCESS_ENUMS_NAME)
                .value(KEY, bindMarker())
                .value(COLUMN1, bindMarker())
                .value(VALUE, bindMarker());
        putValue = DatastaxIO.getSession()
                .prepare(insert)
                .setConsistencyLevel( ConsistencyLevel.ONE);  // TODO: remove later; required by the cassandra-maven-plugin 2.0.0-1
                                                              // (see https://issues.apache.org/jira/browse/CASSANDRA-6238)

    }

    @Override
    public Set<Locator> getExcessEnumMetrics() throws IOException {
        Timer.Context ctx = Instrumentation.getReadTimerContext(CassandraModel.CF_METRICS_EXCESS_ENUMS_NAME);

        Session session = DatastaxIO.getSession();

        final Set<Locator> excessEnumMetrics = new HashSet<Locator>();

        try {
            BoundStatement bs = getValue.bind();
            List<Row> results = session.execute(bs).all();

            for ( Row row : results ) {
                if ( LOG.isTraceEnabled() ) {
                    LOG.trace( "Read metrics_excess_enums: " + row.getString( KEY ));
                }
                excessEnumMetrics.add(Locator.createLocatorFromDbKey(row.getString(KEY)));
            }

            return excessEnumMetrics;
        }
        finally {
            ctx.stop();
        }

    }

    @Override
    public void insertExcessEnumMetric(Locator locator) throws IOException {

        Timer.Context ctx = Instrumentation.getWriteTimerContext(CassandraModel.CF_METRICS_EXCESS_ENUMS_NAME);

        Session session = DatastaxIO.getSession();

        try {
            // bound values and execute
            // inserting null value doesn't work :-(, the driver just silently
            // drop the whole data and nothing got inserted
            BoundStatement bound = putValue.bind(locator.toString(), 0L, 0L);
            session.execute(bound);
        } finally {
            ctx.stop();
        }

    }

}
