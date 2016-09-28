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
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.rackspacecloud.blueflood.cache.TenantTtlProvider;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.Instrumentation;
import com.rackspacecloud.blueflood.io.LocatorIO;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;

/**
 * This class uses the Datastax driver to read/write locators from
 * Cassandra metrics_locator Column Family.
 */
public class DLocatorIO implements LocatorIO {

    private static final Logger LOG = LoggerFactory.getLogger(DLocatorIO.class);

    private static final String KEY = "key";
    private static final String COLUMN1 = "column1";
    private static final String VALUE = "value";

    private PreparedStatement getValue;
    private PreparedStatement putValue;

    /**
     * Constructor
     */
    public DLocatorIO() {
        createPreparedStatements();
    }

    /**
     * Create all prepared statements use in this class for metrics_locator
     */
    private void createPreparedStatements()  {

        // create a generic select statement for retrieving from metrics_locator
        Select.Where select = QueryBuilder
                .select()
                .all()
                .from( CassandraModel.CF_METRICS_LOCATOR_NAME )
                .where( eq ( KEY, bindMarker() ));
        getValue = DatastaxIO.getSession().prepare( select );

        // create a generic insert statement for inserting into metrics_locator
        Insert insert = QueryBuilder.insertInto( CassandraModel.CF_METRICS_LOCATOR_NAME)
                .using(ttl(TenantTtlProvider.LOCATOR_TTL))
                .value(KEY, bindMarker())
                .value(COLUMN1, bindMarker())
                .value(VALUE, bindMarker());
        putValue = DatastaxIO.getSession()
                .prepare(insert)
                .setConsistencyLevel( ConsistencyLevel.ONE );  // TODO: remove later; required by the cassandra-maven-plugin 2.0.0-1
                                                              // (see https://issues.apache.org/jira/browse/CASSANDRA-6238)

    }

    /**
     * Insert a locator with key = shard long value calculated using Util.getShard()
     * @param locator
     * @throws IOException
     */
    @Override
    public void insertLocator(Locator locator) throws IOException {
        Session session = DatastaxIO.getSession();

        // get shard this locator would belong to
        long shard = (long) Util.getShard(locator.toString());

        Timer.Context timer = Instrumentation.getWriteTimerContext(CassandraModel.CF_METRICS_LOCATOR_NAME);
        try {
            // bound values and execute
            BoundStatement bs = putValue.bind(shard, locator.toString(), "");
            session.execute(bs);
        } finally {
            timer.stop();
        }
    }

    /**
     * Returns the locators for a shard, i.e. those that should be rolled up, for a given shard.
     * 'Should' means:
     *  1) A locator is capable of rollup (it is not a string/boolean metric).
     *  2) A locator has had new data in the past LOCATOR_TTL seconds.
     *
     * @param shard Number of the shard you want the locators for. 0-127 inclusive.
     * @return Collection of locators
     * @throws IOException
     */
    @Override
    public Collection<Locator> getLocators(long shard) throws IOException {

        Timer.Context ctx = Instrumentation.getReadTimerContext(CassandraModel.CF_METRICS_LOCATOR_NAME);
        Session session = DatastaxIO.getSession();

        Collection<Locator> locators = new ArrayList<Locator>();

        try {
            // bind value
            BoundStatement bs = getValue.bind(shard);
            List<Row> results = session.execute(bs).all();
            for ( Row row : results ) {
                if ( LOG.isTraceEnabled() ) {
                    LOG.trace( "Read metrics_locators with shard " + shard + ": " +
                            row.getString( KEY ) +
                            row.getString( COLUMN1 ));
                }
                locators.add(Locator.createLocatorFromDbKey(row.getString(COLUMN1)));
            }

            // return results
            if (locators.size() == 0) {
                Instrumentation.markNotFound(CassandraModel.CF_METRICS_LOCATOR_NAME);
                return Collections.emptySet();
            }
            else {
                return locators;
            }
        } finally {
            ctx.stop();
        }
    }

}
