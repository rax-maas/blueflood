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
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.io.serializers.metrics.EnumSerDes;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Locators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * This class holds the utility methods to read/write enum metrics
 * using Datastax driver.
 */
public class DatastaxEnumIO implements AsyncWriter, EnumReader {

    private static final Logger LOG = LoggerFactory.getLogger(DatastaxEnumIO.class);
    private static final EnumSerDes serDes = new EnumSerDes();

    private static final String INSERT_KEY_COLUMN_VALUE_FORMAT = "INSERT INTO %s (key, column1, value) VALUES (?, ?, ?)";
    private static final String SELECT_FOR_KEY = "SELECT * FROM %s WHERE key = :locator";
    private static final String SELECT_FOR_KEY_RANGE_FORMAT = "SELECT * FROM %s WHERE key = :locator AND column1 >= :tsStart AND column1 <= :tsEnd";

    private PreparedStatement insertToMetricsEnumStatement;
    private PreparedStatement insertToMetricsPreaggrFullStatement;
    private PreparedStatement insertToMetricsPreaggr5MStatement;
    private PreparedStatement insertToMetricsPreaggr20MStatement;
    private PreparedStatement insertToMetricsPreaggr60MStatement;
    private PreparedStatement insertToMetricsPreaggr240MStatement;
    private PreparedStatement insertToMetricsPreaggr1440MStatement;

    private PreparedStatement selectFromMetricsEnumStatement;
    private PreparedStatement selectFromMetricsPreaggrFullForRangeStatement;
    private PreparedStatement selectFromMetricsPreaggr5MForRangeStatement;
    private PreparedStatement selectFromMetricsPreaggr20MForRangeStatement;
    private PreparedStatement selectFromMetricsPreaggr60MForRangeStatement;
    private PreparedStatement selectFromMetricsPreaggr240MForRangeStatement;
    private PreparedStatement selectFromMetricsPreaggr1440MForRangeStatement;

    private Map<String, PreparedStatement> cfNameToSelectStatement;
    private Map<Granularity, PreparedStatement> granToInsertStatement;

    /**
     * Constructor
     */
    public DatastaxEnumIO() {
        Session session = DatastaxIO.getSession();

        insertToMetricsEnumStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_FORMAT,
                        CassandraModel.CF_METRICS_ENUM_NAME));
        insertToMetricsPreaggrFullStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_FULL_NAME));
        insertToMetricsPreaggr5MStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_5M_NAME));
        insertToMetricsPreaggr20MStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_20M_NAME));
        insertToMetricsPreaggr60MStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_60M_NAME));
        insertToMetricsPreaggr240MStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_240M_NAME));
        insertToMetricsPreaggr1440MStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_1440M_NAME));

        selectFromMetricsEnumStatement = session.prepare(
                String.format(SELECT_FOR_KEY,
                        CassandraModel.CF_METRICS_ENUM_NAME));
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

        granToInsertStatement = new HashMap<Granularity, PreparedStatement>() {{
            put(Granularity.FULL, insertToMetricsPreaggrFullStatement);
            put(Granularity.MIN_5, insertToMetricsPreaggr5MStatement);
            put(Granularity.MIN_20, insertToMetricsPreaggr20MStatement);
            put(Granularity.MIN_60, insertToMetricsPreaggr60MStatement);
            put(Granularity.MIN_240, insertToMetricsPreaggr240MStatement);
            put(Granularity.MIN_1440, insertToMetricsPreaggr1440MStatement);
        }};
    }

    /**
     * Read the metrics_enum column family for the specified locators. Organize
     * the data as a table of locator, enum value hash, and enum value.
     * This is a representation on how the data looks in the column family.
     *
     * @param locators
     * @return
     */
    public Table<Locator, Long, String> getEnumHashValuesForLocators(final List<Locator> locators) {

        Timer.Context ctx = Instrumentation.getReadTimerContext(CassandraModel.CF_METRICS_ENUM_NAME);
        Session session = DatastaxIO.getSession();

        Table<Locator, Long, String> locatorEnumHashValues = HashBasedTable.create();
        List<ResultSetFuture> resultsFuture = new ArrayList<ResultSetFuture>();

        try {
            for (String locatorStr : Locators.toStringList(locators)) {
                resultsFuture.add(session.executeAsync(selectFromMetricsEnumStatement.bind(locatorStr)));
            }

            for (ResultSetFuture future : resultsFuture) {
                try {
                    List<Row> results = future.get().all();
                    for (Row row : results) {
                        String key = row.getString(AbstractMetricsIO.KEY);
                        Locator locator = Locator.createLocatorFromDbKey(key);

                        Long hash = row.getLong(AbstractMetricsIO.COLUMN1);
                        String enumValue = row.getString(AbstractMetricsIO.VALUE);
                        locatorEnumHashValues.put(locator, hash, enumValue);
                    }
                } catch (InterruptedException ex) {
                    Instrumentation.markReadError();
                    LOG.error("Interrupted error querying enum from " + CassandraModel.CF_METRICS_ENUM_NAME, ex);
                } catch (ExecutionException ex) {
                    Instrumentation.markReadError();
                    LOG.error("Execution error querying enum from " + CassandraModel.CF_METRICS_ENUM_NAME, ex);
                }
            }
        } finally {
            ctx.stop();
        }
        return locatorEnumHashValues;
    }

    /**
     * Locates all enum values from metrics_enum column family by their
     * {@link com.rackspacecloud.blueflood.types.Locator}.
     *
     * The result is organized in a map of Locator -> list of String. The string
     * is the string enum values.
     *
     * @param locators
     * @return
     */
    @Override
    public Map<Locator, List<String>> getEnumStringMappings(final List<Locator> locators) {
        final Table<Locator, Long, String> locatorEnumHashValues = getEnumHashValuesForLocators(locators);
        return new HashMap<Locator, List<String>>() {{
            for ( Locator locator : locatorEnumHashValues.rowKeySet() ) {
                put(locator, new ArrayList<String>(locatorEnumHashValues.row(locator).values()));
            }
        }};
    }

    /**
     * Read the metrics_preaggregated_{granularity} for specific locators.
     * This is to be implemented by driver specific class.
     *
     * @param locators
     * @param columnFamily
     * @param range
     * @return
     */
    public Table<Locator, Long, BluefloodEnumRollup> getEnumRollupsForLocators(final List<Locator> locators, String columnFamily, Range range) {
        Timer.Context ctx = Instrumentation.getReadTimerContext(columnFamily);
        Session session = DatastaxIO.getSession();

        Table<Locator, Long, BluefloodEnumRollup> locatorHashRollup = HashBasedTable.create();
        List<ResultSetFuture> resultsFuture = new ArrayList<ResultSetFuture>();

        try {
            for (String locatorStr : Locators.toStringList(locators)) {
                PreparedStatement statement = cfNameToSelectStatement.get(columnFamily);
                statement.bind(locatorStr, range.getStart(), range.getStop());
                resultsFuture.add(session.executeAsync(statement.bind(locatorStr, range.getStart(), range.getStop())));
            }

            for (ResultSetFuture future : resultsFuture) {
                try {
                    List<Row> results = future.get().all();
                    for (Row row : results) {
                        String key = row.getString(AbstractMetricsIO.KEY);
                        Locator locator = Locator.createLocatorFromDbKey(key);
                        locatorHashRollup.put(locator, row.getLong(AbstractMetricsIO.COLUMN1),
                                serDes.deserialize(row.getBytes(AbstractMetricsIO.VALUE)));
                    }
                } catch (InterruptedException ex) {
                    Instrumentation.markReadError();
                    LOG.error("Interrupted error querying enum from " + columnFamily, ex);
                } catch (ExecutionException ex) {
                    Instrumentation.markReadError();
                    LOG.error("Execution error querying enum from " + columnFamily, ex);
                }
            }
        } finally {
            ctx.stop();
        }
        return locatorHashRollup;
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
    @Override
    public ResultSetFuture putAsync(Locator locator, long collectionTime, Rollup rollup, Granularity granularity) {

        if (!(rollup instanceof BluefloodEnumRollup)) {
            throw new IllegalArgumentException(
                    String.format("Found metric with locator=%s having rollupType enum but object instance of %s, skipping write",
                        locator, rollup.getClass().getSimpleName()));
        }

        BluefloodEnumRollup enumRollup = (BluefloodEnumRollup) rollup;
        Timer.Context ctx = Instrumentation.getWriteTimerContext(granularity.name());
        Session session = DatastaxIO.getSession();
        try {
            // The assumption is that the key (i.e: locator), if they are the same
            // for multiple column family, they will still go to the same partition.
            // In this case, enum is inserted to metrics_enum and metrics_preaggregated_full.
            // As long as the locator is the same, we are writing them in one batch to
            // ensure some atomicity.
            BatchStatement batch = new BatchStatement();
            addInsertMetricsEnumToBatch(batch, locator, enumRollup);
            addInsertMetricsPreaggregatedToBatch(batch, locator, collectionTime, enumRollup, granularity);

            ResultSetFuture result = session.executeAsync(batch);
            return result;
        } finally {
            ctx.stop();
        }
    }

    /**
     * Add {@link com.datastax.driver.core.PreparedStatement} statement(s) to the
     * {@link com.datastax.driver.core.BatchStatement} to insert this Enum to
     * metrics_enum column family
     *
     * @param batch
     * @param locator
     * @param enumRollup
     */
    private void addInsertMetricsEnumToBatch(BatchStatement batch, Locator locator, BluefloodEnumRollup enumRollup) {
        for ( String enumValue : enumRollup.getStringEnumValuesWithCounts().keySet() ) {
            batch.add(insertToMetricsEnumStatement.bind(locator.toString(), (long)enumValue.hashCode(), enumValue));
        }
    }

    /**
     * Add a {@link com.datastax.driver.core.PreparedStatement} statement to the
     * {@link com.datastax.driver.core.BatchStatement} to insert this Enum to
     * metrics_preaggregated_{granularity} column family
     *
     * @param batch
     * @param locator
     * @param collectionTime
     * @param enumRollup
     * @param granularity
     */
    private void addInsertMetricsPreaggregatedToBatch(BatchStatement batch, Locator locator, long collectionTime,
                                                      BluefloodEnumRollup enumRollup,
                                                      Granularity granularity) {
        PreparedStatement statement = granToInsertStatement.get(granularity);
        BoundStatement bound = statement.bind(locator.toString(),
                                                collectionTime,
                                                serDes.serialize(enumRollup));
        batch.add(bound);
    }
}
