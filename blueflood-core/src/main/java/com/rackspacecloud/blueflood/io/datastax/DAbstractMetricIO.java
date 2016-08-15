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

import com.datastax.driver.core.*;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.Instrumentation;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Long;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * This is an abstract class that collects the common behavior of
 * reading/writing preaggregated & basic numeric metrics objects.
 */
public abstract class DAbstractMetricIO {

    private static final Logger LOG = LoggerFactory.getLogger(DAbstractMetricIO.class);

    protected Session session;

    protected final DMetricsCFPreparedStatements metricsCFPreparedStatements;

    protected DAbstractMetricIO() {
        metricsCFPreparedStatements = DMetricsCFPreparedStatements.getInstance();
        session = DatastaxIO.getSession();
    }

    /**
     * Asynchronously insert a rolled up metric to the appropriate column family
     * for a particular granularity
     *
     * @param locator
     * @param collectionTime
     * @param rollup
     * @param granularity
     * @return
     */
    public ResultSetFuture putAsync(Locator locator, long collectionTime, Rollup rollup, Granularity granularity, int ttl) {

        Session session = DatastaxIO.getSession();

        // we use batch statement here in case sub classes
        // override the addRollupToBatch() and provide
        // multiple statements
        BatchStatement batch = new BatchStatement();
        addRollupToBatch(batch, locator, rollup, collectionTime, granularity, ttl);

        Collection<Statement> statements = batch.getStatements();
        if ( statements.size() == 1 ) {
            Statement oneStatement = statements.iterator().next();
            return session.executeAsync(oneStatement);
        } else {
            LOG.debug(String.format("Using BatchStatement for %d statements", statements.size()));
            return session.executeAsync(batch);
        }
    }

    public Statement createStatement(Locator locator, long collectionTime, Rollup rollup, Granularity granularity, int ttl) {
        final PreparedStatement statement;

        if( rollup.getRollupType() == RollupType.BF_BASIC ) {

            // Strings and Booleans don't get rolled up.  I'd like to verify
            // that none are passed in, but that would require a db access

            statement = metricsCFPreparedStatements.basicGranToInsertStatement.get( granularity );
        }
        else {
            statement = metricsCFPreparedStatements.preaggrGranToInsertStatement.get(granularity);
        }

        BoundStatement bound = statement.bind(locator.toString(),
                collectionTime,
                toByteBuffer(rollup),
                ttl);

        return bound;
    }

    /**
     * Fetch rollup objects for a {@link com.rackspacecloud.blueflood.types.Locator}
     * from the specified column family and range.
     *
     * @param locator
     * @param columnFamily
     * @param range
     * @return
     */
    protected <T extends Object> Table<Locator, Long, T> getRollupsForLocator(final Locator locator,
                                                                              String columnFamily,
                                                                              Range range) {
        return getValuesForLocators( new ArrayList<Locator>() {{
            add( locator );
        }}, columnFamily, range );
    }

    /**
     * Fetch values for a list of {@link com.rackspacecloud.blueflood.types.Locator}
     * from the specified column family and range.
     *
     * This is a base behavior for most rollup types. IO subclasses can override
     * this behavior as they see fit.
     *
     * @param locators
     * @param columnFamily
     * @param range
     * @return
     */
    protected <T extends Object> Table<Locator, Long, T> getValuesForLocators( final List<Locator> locators,
                                                                               String columnFamily,
                                                                               Range range ) {

        Table<Locator, Long, T> locatorTimestampRollup = HashBasedTable.create();

        Map<Locator, List<ResultSetFuture>> resultSetFuturesMap = selectForLocatorListAndRange(columnFamily, locators, range);

        for (Map.Entry<Locator, List<ResultSetFuture>> entry : resultSetFuturesMap.entrySet() ) {
            Locator locator = entry.getKey();
            List<ResultSetFuture> futures = entry.getValue();

            Table<Locator, Long, T> result = toLocatorTimestampValue( futures, locator, CassandraModel.getGranularity( columnFamily ) );
            locatorTimestampRollup.putAll(result);
        }
        return locatorTimestampRollup;
    }

    /**
     * Provides a way for the sub class to get a {@link java.nio.ByteBuffer}
     * representation of a certain value.
     *
     * @param value
     * @return
     */
    protected abstract <T extends Object> ByteBuffer toByteBuffer(T value);

    /**
     * Provides a way for the sub class to construct the right Rollup
     * object from a {@link java.nio.ByteBuffer}
     *
     * @param byteBuffer
     * @return
     */
    protected abstract <T extends Object> T fromByteBuffer(ByteBuffer byteBuffer);

    /**
     * Add a {@link com.datastax.driver.core.PreparedStatement} statement to the
     * {@link com.datastax.driver.core.BatchStatement} to insert this Rollup
     * object to metrics_preaggregated_{granularity} column family
     *
     * @param batch
     * @param locator
     * @param collectionTime
     * @param granularity
     * @param ttl
     */
    protected void addRollupToBatch(BatchStatement batch, Locator locator,
                                    Rollup rollup,
                                    long collectionTime,
                                    Granularity granularity,
                                    int ttl) {
        Statement statement = createStatement(locator, collectionTime, rollup, granularity, ttl);
        batch.add(statement);
    }

    /**
     * Asynchronously execute select statements against the specified
     * column family for a specific list of
     * {@link com.rackspacecloud.blueflood.types.Locator} and
     * {@link com.rackspacecloud.blueflood.types.Range}
     *
     * @param locators
     * @param range
     * @return a map of Locator -> a list of ResultSetFuture
     */
    protected Map<Locator, List<ResultSetFuture>> selectForLocatorListAndRange(String columnFamilyName,
                                                                               List<Locator> locators,
                                                                               Range range) {
        Map<Locator, List<ResultSetFuture>> locatorFuturesMap = new HashMap<Locator, List<ResultSetFuture>>();
        for (Locator locator : locators) {
            List<ResultSetFuture> existing = locatorFuturesMap.get(locator);
            if ( existing == null ) {
                existing = new ArrayList<ResultSetFuture>();
                locatorFuturesMap.put(locator, existing);
            }
            existing.addAll(selectForLocatorAndRange(columnFamilyName, locator, range));
        }
        return locatorFuturesMap;
    }

    /**
     * Execute a select statement against the specified column family for a specific
     * {@link com.rackspacecloud.blueflood.types.Locator} and
     * {@link com.rackspacecloud.blueflood.types.Range}
     *
     * @param columnFamily the column family name to do select against
     * @param locator
     * @param range

     * @return
     */
    protected List<ResultSetFuture> selectForLocatorAndRange(String columnFamily, Locator locator, Range range) {
        List<ResultSetFuture> resultsFutures = new ArrayList<ResultSetFuture>();
        PreparedStatement statement = metricsCFPreparedStatements.cfNameToSelectStatement.get(columnFamily);
        resultsFutures.add(
                session.executeAsync(statement.bind(locator.toString(), range.getStart(), range.getStop())));
        return resultsFutures;
    }

    /**
     *  Give a {@link com.datastax.driver.core.ResultSetFuture}, get
     *  the corresponding data from it and return it as a
     *  Table of locator, long and rollup.
     */
    public <T extends Object> Table<Locator, Long, T> toLocatorTimestampValue( List<ResultSetFuture> futures,
                                                                               Locator locator,
                                                                               Granularity granularity ) {
        Table<Locator, Long, T> locatorTimestampRollup = HashBasedTable.create();
        for ( ResultSetFuture future : futures ) {
            try {
                List<Row> rows = future.getUninterruptibly().all();
                for (Row row : rows) {
                    String key = row.getString(DMetricsCFPreparedStatements.KEY);
                    Locator loc = Locator.createLocatorFromDbKey(key);
                    Long hash = row.getLong(DMetricsCFPreparedStatements.COLUMN1);
                    locatorTimestampRollup.put(loc, hash, (T)fromByteBuffer(row.getBytes(DMetricsCFPreparedStatements.VALUE)));
                }
            } catch (Exception ex) {
                Instrumentation.markReadError();
                LOG.error(String.format("error reading metric for locator %s, granularity %s",
                        locator, granularity), ex);
            }
        }
        return locatorTimestampRollup;
    }
}
