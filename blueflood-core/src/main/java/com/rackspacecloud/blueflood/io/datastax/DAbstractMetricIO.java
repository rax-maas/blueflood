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

    /**
     * The key of the metrics_preaggregated_* Column Families
     */
    public static final String KEY = "key";

    /**
     * The name of the first column
     */
    public static final String COLUMN1 = "column1";

    /**
     * The name of the value column
     */
    public static final String VALUE = "value";

    protected static final String INSERT_KEY_COLUMN_VALUE_FORMAT = "INSERT INTO %s (key, column1, value) VALUES (?, ?, ?)";
    protected static final String INSERT_KEY_COLUMN_VALUE_TTL_FORMAT = "INSERT INTO %s (key, column1, value) VALUES (?, ?, ?) USING TTL ?";
    protected static final String SELECT_FOR_KEY = "SELECT * FROM %s WHERE key = :locator";
    protected static final String SELECT_FOR_KEY_RANGE_FORMAT = "SELECT * FROM %s WHERE key = :locator AND column1 >= :tsStart AND column1 <= :tsEnd";

    private static final Logger LOG = LoggerFactory.getLogger(DAbstractMetricIO.class);

    protected final PreparedStatement insertToMetricsPreaggrFullStatement;
    protected final PreparedStatement insertToMetricsPreaggr5MStatement;
    protected final PreparedStatement insertToMetricsPreaggr20MStatement;
    protected final PreparedStatement insertToMetricsPreaggr60MStatement;
    protected final PreparedStatement insertToMetricsPreaggr240MStatement;
    protected final PreparedStatement insertToMetricsPreaggr1440MStatement;

    private final PreparedStatement selectFromMetricsPreaggrFullForRangeStatement;
    private final PreparedStatement selectFromMetricsPreaggr5MForRangeStatement;
    private final PreparedStatement selectFromMetricsPreaggr20MForRangeStatement;
    private final PreparedStatement selectFromMetricsPreaggr60MForRangeStatement;
    private final PreparedStatement selectFromMetricsPreaggr240MForRangeStatement;
    private final PreparedStatement selectFromMetricsPreaggr1440MForRangeStatement;

    protected final PreparedStatement insertToMetricsBasicFullStatement;
    protected final PreparedStatement insertToMetricsBasic5MStatement;
    protected final PreparedStatement insertToMetricsBasic20MStatement;
    protected final PreparedStatement insertToMetricsBasic60MStatement;
    protected final PreparedStatement insertToMetricsBasic240MStatement;
    protected final PreparedStatement insertToMetricsBasic1440MStatement;

    private final PreparedStatement selectFromMetricsStringForRangeStatement;

    private final PreparedStatement selectFromMetricsBasicFullForRangeStatement;
    private final PreparedStatement selectFromMetricsBasic5MForRangeStatement;
    private final PreparedStatement selectFromMetricsBasic20MForRangeStatement;
    private final PreparedStatement selectFromMetricsBasic60MForRangeStatement;
    private final PreparedStatement selectFromMetricsBasic240MForRangeStatement;
    private final PreparedStatement selectFromMetricsBasic1440MForRangeStatement;

    protected Session session;
    protected Map<String, PreparedStatement> cfNameToSelectStatement;
    protected Map<Granularity, PreparedStatement> preaggrGranToInsertStatement;
    protected Map<Granularity, PreparedStatement> basicGranToInsertStatement;

    protected DAbstractMetricIO() {
        session = DatastaxIO.getSession();

        //
        // Preaggr insert statements
        //
        insertToMetricsPreaggrFullStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_TTL_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_FULL_NAME));
        insertToMetricsPreaggr5MStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_TTL_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_5M_NAME));
        insertToMetricsPreaggr20MStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_TTL_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_20M_NAME));
        insertToMetricsPreaggr60MStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_TTL_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_60M_NAME));
        insertToMetricsPreaggr240MStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_TTL_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_240M_NAME));
        insertToMetricsPreaggr1440MStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_TTL_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_1440M_NAME));

        //
        // Preaggr select statements
        //
        selectFromMetricsPreaggrFullForRangeStatement = session.prepare(
                String.format(SELECT_FOR_KEY_RANGE_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_FULL_NAME));
        selectFromMetricsPreaggr5MForRangeStatement = session.prepare(
                String.format(SELECT_FOR_KEY_RANGE_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_5M_NAME));
        selectFromMetricsPreaggr20MForRangeStatement = session.prepare(
                String.format(SELECT_FOR_KEY_RANGE_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_20M_NAME));
        selectFromMetricsPreaggr60MForRangeStatement = session.prepare(
                String.format(SELECT_FOR_KEY_RANGE_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_60M_NAME));
        selectFromMetricsPreaggr240MForRangeStatement = session.prepare(
                String.format(SELECT_FOR_KEY_RANGE_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_240M_NAME));
        selectFromMetricsPreaggr1440MForRangeStatement = session.prepare(
                String.format(SELECT_FOR_KEY_RANGE_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_1440M_NAME));

        cfNameToSelectStatement = new HashMap<String, PreparedStatement>() {{
            put(CassandraModel.CF_METRICS_PREAGGREGATED_FULL_NAME, selectFromMetricsPreaggrFullForRangeStatement);
            put(CassandraModel.CF_METRICS_PREAGGREGATED_5M_NAME, selectFromMetricsPreaggr5MForRangeStatement);
            put(CassandraModel.CF_METRICS_PREAGGREGATED_20M_NAME, selectFromMetricsPreaggr20MForRangeStatement);
            put(CassandraModel.CF_METRICS_PREAGGREGATED_60M_NAME, selectFromMetricsPreaggr60MForRangeStatement);
            put(CassandraModel.CF_METRICS_PREAGGREGATED_240M_NAME, selectFromMetricsPreaggr240MForRangeStatement);
            put(CassandraModel.CF_METRICS_PREAGGREGATED_1440M_NAME, selectFromMetricsPreaggr1440MForRangeStatement);
        }};

        preaggrGranToInsertStatement = new HashMap<Granularity, PreparedStatement>() {{
            put(Granularity.FULL, insertToMetricsPreaggrFullStatement);
            put(Granularity.MIN_5, insertToMetricsPreaggr5MStatement);
            put(Granularity.MIN_20, insertToMetricsPreaggr20MStatement);
            put(Granularity.MIN_60, insertToMetricsPreaggr60MStatement);
            put(Granularity.MIN_240, insertToMetricsPreaggr240MStatement);
            put(Granularity.MIN_1440, insertToMetricsPreaggr1440MStatement);
        }};

        //
        // Basic insert statements
        //
        insertToMetricsBasicFullStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_TTL_FORMAT,
                        CassandraModel.CF_METRICS_FULL_NAME));
        insertToMetricsBasic5MStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_TTL_FORMAT,
                        CassandraModel.CF_METRICS_5M_NAME));
        insertToMetricsBasic20MStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_TTL_FORMAT,
                        CassandraModel.CF_METRICS_20M_NAME));
        insertToMetricsBasic60MStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_TTL_FORMAT,
                        CassandraModel.CF_METRICS_60M_NAME));
        insertToMetricsBasic240MStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_TTL_FORMAT,
                        CassandraModel.CF_METRICS_240M_NAME));
        insertToMetricsBasic1440MStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_TTL_FORMAT,
                        CassandraModel.CF_METRICS_1440M_NAME));

        //
        // Basic select statements
        //
        selectFromMetricsStringForRangeStatement = session.prepare(
                String.format(SELECT_FOR_KEY_RANGE_FORMAT,
                        CassandraModel.CF_METRICS_STRING_NAME));
        selectFromMetricsBasicFullForRangeStatement = session.prepare(
                String.format(SELECT_FOR_KEY_RANGE_FORMAT,
                        CassandraModel.CF_METRICS_FULL_NAME));
        selectFromMetricsBasic5MForRangeStatement = session.prepare(
                String.format(SELECT_FOR_KEY_RANGE_FORMAT,
                        CassandraModel.CF_METRICS_5M_NAME));
        selectFromMetricsBasic20MForRangeStatement = session.prepare(
                String.format(SELECT_FOR_KEY_RANGE_FORMAT,
                        CassandraModel.CF_METRICS_20M_NAME));
        selectFromMetricsBasic60MForRangeStatement = session.prepare(
                String.format(SELECT_FOR_KEY_RANGE_FORMAT,
                        CassandraModel.CF_METRICS_60M_NAME));
        selectFromMetricsBasic240MForRangeStatement = session.prepare(
                String.format(SELECT_FOR_KEY_RANGE_FORMAT,
                        CassandraModel.CF_METRICS_240M_NAME));
        selectFromMetricsBasic1440MForRangeStatement = session.prepare(
                String.format(SELECT_FOR_KEY_RANGE_FORMAT,
                        CassandraModel.CF_METRICS_1440M_NAME));

        cfNameToSelectStatement.put( CassandraModel.CF_METRICS_STRING_NAME, selectFromMetricsStringForRangeStatement );

        cfNameToSelectStatement.put( CassandraModel.CF_METRICS_FULL_NAME, selectFromMetricsBasicFullForRangeStatement );
        cfNameToSelectStatement.put( CassandraModel.CF_METRICS_5M_NAME, selectFromMetricsBasic5MForRangeStatement );
        cfNameToSelectStatement.put( CassandraModel.CF_METRICS_20M_NAME, selectFromMetricsBasic20MForRangeStatement );
        cfNameToSelectStatement.put( CassandraModel.CF_METRICS_60M_NAME, selectFromMetricsBasic60MForRangeStatement );
        cfNameToSelectStatement.put( CassandraModel.CF_METRICS_240M_NAME, selectFromMetricsBasic240MForRangeStatement );
        cfNameToSelectStatement.put(CassandraModel.CF_METRICS_1440M_NAME, selectFromMetricsBasic1440MForRangeStatement);

        basicGranToInsertStatement = new HashMap<Granularity, PreparedStatement>() {{
            // NOTE:  this shoudn't be called.  explain why later
            put(Granularity.FULL, insertToMetricsBasicFullStatement );
            put(Granularity.MIN_5, insertToMetricsBasic5MStatement);
            put(Granularity.MIN_20, insertToMetricsBasic20MStatement);
            put(Granularity.MIN_60, insertToMetricsBasic60MStatement);
            put(Granularity.MIN_240, insertToMetricsBasic240MStatement);
            put(Granularity.MIN_1440, insertToMetricsBasic1440MStatement);
        }};
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
        return session.executeAsync(batch);
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
        PreparedStatement statement;

        if( rollup.getRollupType() == RollupType.BF_BASIC ) {

            // Strings and Booleans don't get rolled up.  I'd like to verify
            // that none are passed in, but that would require a db access

            statement = basicGranToInsertStatement.get( granularity );
        }
        else {
            statement = preaggrGranToInsertStatement.get(granularity);
        }

        BoundStatement bound = statement.bind(locator.toString(),
                                    collectionTime,
                                    toByteBuffer(rollup),
                                    ttl);
        batch.add(bound);
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
        PreparedStatement statement = cfNameToSelectStatement.get(columnFamily);
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
                    String key = row.getString(KEY);
                    Locator loc = Locator.createLocatorFromDbKey(key);
                    Long hash = row.getLong(COLUMN1);
                    locatorTimestampRollup.put(loc, hash, (T)fromByteBuffer(row.getBytes(VALUE)));
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
