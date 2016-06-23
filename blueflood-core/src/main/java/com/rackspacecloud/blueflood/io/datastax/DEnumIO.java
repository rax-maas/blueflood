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
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.io.serializers.metrics.EnumSerDes;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.LocatorsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * This class holds the utility methods to read/write enum metrics
 * using Datastax driver.
 */
public class DEnumIO extends DAbstractMetricIO implements EnumReaderIO {

    protected static final String INSERT_KEY_COLUMN_VALUE_FORMAT = "INSERT INTO %s (key, column1, value) VALUES (?, ?, ?)";
    protected static final String SELECT_FOR_KEY = "SELECT * FROM %s WHERE key = :locator";

    private static final Logger LOG = LoggerFactory.getLogger(DEnumIO.class);
    private static final EnumSerDes serDes = new EnumSerDes();

    private final PreparedStatement insertToMetricsEnumStatement;
    private final PreparedStatement selectFromMetricsEnumStatement;

    /**
     * Constructor
     */
    public DEnumIO() {
        Session session = DatastaxIO.getSession();

        insertToMetricsEnumStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_FORMAT,
                        CassandraModel.CF_METRICS_ENUM_NAME));

        selectFromMetricsEnumStatement = session.prepare(
                String.format(SELECT_FOR_KEY,
                        CassandraModel.CF_METRICS_ENUM_NAME));

        metricsCFPreparedStatements.cfNameToSelectStatement.put(CassandraModel.CF_METRICS_ENUM_NAME, selectFromMetricsEnumStatement);
    }

    /**
     * Read the metrics_enum column family for the specified locators. Organize
     * the data as a table of locator, enum value hash, and enum value.
     * This is a representation on how the data looks in the column family.
     *
     * @param locators
     * @return
     */
    @Override
    public Table<Locator, Long, String> getEnumHashValuesForLocators(final List<Locator> locators) {

        Timer.Context ctx = Instrumentation.getReadTimerContext(CassandraModel.CF_METRICS_ENUM_NAME);
        Session session = DatastaxIO.getSession();

        Table<Locator, Long, String> locatorEnumHashValues = HashBasedTable.create();
        List<ResultSetFuture> resultsFuture = new ArrayList<ResultSetFuture>();

        try {
            for (String locatorStr : LocatorsUtils.toStringList(locators)) {
                resultsFuture.add(session.executeAsync(selectFromMetricsEnumStatement.bind(locatorStr)));
            }

            for (ResultSetFuture future : resultsFuture) {
                try {
                    List<Row> results = future.getUninterruptibly().all();
                    for (Row row : results) {
                        String key = row.getString(metricsCFPreparedStatements.KEY);
                        Locator locator = Locator.createLocatorFromDbKey(key);

                        Long hash = row.getLong(metricsCFPreparedStatements.COLUMN1);
                        String enumValue = row.getString(metricsCFPreparedStatements.VALUE);
                        locatorEnumHashValues.put(locator, hash, enumValue);
                    }
                } catch (Exception ex) {
                    Instrumentation.markReadError();
                    LOG.error("error querying enum from " + CassandraModel.CF_METRICS_ENUM_NAME, ex);
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
    @Override
    public Table<Locator, Long, BluefloodEnumRollup> getEnumRollupsForLocators(final List<Locator> locators,
                                                                               String columnFamily,
                                                                               Range range) {
        return getValuesForLocators( locators, columnFamily, range );
    }

    /**
     * Provides the serialized {@link java.nio.ByteBuffer} representation
     * of the specified {@link com.rackspacecloud.blueflood.types.Rollup}
     * object
     *
     * @param value
     * @return
     */
    @Override
    protected ByteBuffer toByteBuffer(Object value) {
        if ( ! (value instanceof BluefloodEnumRollup) ) {
            // or throw new ShouldNotHappenException()
            throw new IllegalArgumentException("toByteBuffer(): expecting BluefloodEnumRollup class but got "
                    + value.getClass().getSimpleName());
        }
        BluefloodEnumRollup enumRollup = (BluefloodEnumRollup) value;
        return serDes.serialize(enumRollup);
    }

    /**
     * Provides a way for the sub class to construct the right Rollup
     * object from a {@link java.nio.ByteBuffer}
     *
     * @param byteBuffer
     * @return
     */
    @Override
    protected BluefloodEnumRollup fromByteBuffer(ByteBuffer byteBuffer) {
        return serDes.deserialize(byteBuffer);
    }

    /**
     * This method overrides the super class method because we need to
     * also insert the enum hash -> enum string mapping to metrics_enum
     * column family.
     *
     * @param batch
     * @param locator
     * @param rollup
     * @param collectionTime
     * @param granularity
     */
    @Override
    protected void addRollupToBatch(BatchStatement batch, Locator locator,
                                    Rollup rollup,
                                    long collectionTime,
                                    Granularity granularity,
                                    int ttl) {
        // The assumption is that the key (i.e: locator), if they are the same
        // for multiple column family, they will still go to the same partition.
        // In this case, enum is inserted to metrics_enum and metrics_preaggregated_full.
        // As long as the locator is the same, we are writing them in one batch to
        // ensure some atomicity.
        addInsertMetricsEnumToBatch(batch, locator, (BluefloodEnumRollup) rollup);
        super.addRollupToBatch(batch, locator, rollup, collectionTime, granularity, ttl);
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
     * Execute a select statement against the specified column family for a specific
     * {@link com.rackspacecloud.blueflood.types.Locator} and
     * {@link com.rackspacecloud.blueflood.types.Range}.
     *
     * For enum metrics, 2 queries against 2 different column families must happened.
     * Thus we are overriding the parent's method here.
     *
     * @param columnFamily
     * @param locator
     * @param range
     * @return
     */
    @Override // DAbstractPreaggregatedIO
    protected List<ResultSetFuture> selectForLocatorAndRange(String columnFamily, Locator locator, Range range) {

        // For enum, we have to do 2 selects, one against metrics_enum, another against
        // the metrics_preaggregated_{gran}. So we will batch these 2 selects into one
        // BatchStatement.

        // first query the metrics_preaggregated_{gran}
        List<ResultSetFuture> resultsFutures = super.selectForLocatorAndRange(columnFamily, locator, range);

        // then, the select against metrics_enum
        ResultSetFuture future = session.executeAsync(selectFromMetricsEnumStatement.bind(locator.toString()));
        resultsFutures.add(future);

        return resultsFutures;
    }

    /**
     *  Give a {@link com.datastax.driver.core.ResultSetFuture}, get
     *  the corresponding data from it and return it as a
     *  Table of locator, long and rollup.
     *
     *  For enum metrics, we need to do a join for between two
     *  ResultSetFuture, one from metrics_enum, one from
     *  metrics_preaggregated_{gran}
     */
    @Override
    public <T extends Object> Table<Locator, Long, T> toLocatorTimestampValue( List<ResultSetFuture> futures,
                                                                               Locator locator,
                                                                               Granularity granularity ) {
        Table<Locator, Long, T> locatorTimestampRollup = HashBasedTable.create();
        Map<Long, String> hashValueMap = new HashMap<Long, String>();
        for ( ResultSetFuture future : futures ) {
            try {
                ResultSet rs = future.getUninterruptibly();
                // get the table that the 'key' column resides
                String table = rs.getColumnDefinitions().getTable(metricsCFPreparedStatements.KEY);
                for ( Row row : rs.all() ) {
                    String key = row.getString(metricsCFPreparedStatements.KEY);
                    Locator loc = Locator.createLocatorFromDbKey(key);
                    Long column1 = row.getLong(metricsCFPreparedStatements.COLUMN1);
                    if ( CassandraModel.CF_METRICS_ENUM_NAME.equals(table) ) {
                        // store a mapping of enum hash to its enum string value
                        hashValueMap.put(column1, row.getString(metricsCFPreparedStatements.VALUE));
                    } else {
                        locatorTimestampRollup.put(loc, column1, (T)fromByteBuffer(row.getBytes(metricsCFPreparedStatements.VALUE)));
                    }
                }
            } catch (Exception ex) {
                Instrumentation.markReadError();
                LOG.error(String.format("Execution error reading preaggregated metric for locator %s, granularity %s",
                        locator, granularity), ex);
            }
        }
        populateEnumValueToCountMap((Map<Long, BluefloodEnumRollup>)locatorTimestampRollup.row(locator), hashValueMap);
        return locatorTimestampRollup;
    }

    /**
     * What we store in Cassandra is the serialized enum rollup object. We only serialize the enum hash code to its count
     * in Cassandra. But {@link com.rackspacecloud.blueflood.types.BluefloodEnumRollup} object has an internal
     * {@link java.util.Map} of enum string to its count. This method is to populate that Map by cross referencing
     * the enum hash code with its enum string, read from metrics_enum column family.
     *
     * @param enumRollupMap      a map of timestamp -> {@link com.rackspacecloud.blueflood.types.BluefloodEnumRollup}
     * @param enumHashValuesMap  a map of enum hash code -> enum string value
     */
    private void populateEnumValueToCountMap(Map<Long, BluefloodEnumRollup> enumRollupMap, Map<Long, String> enumHashValuesMap) {
        for (BluefloodEnumRollup enumRollup : enumRollupMap.values() ) {
            for ( Map.Entry<Long, Long> hashCount: enumRollup.getHashedEnumValuesWithCounts().entrySet()){
                Long hash = hashCount.getKey();
                enumRollup.getStringEnumValuesWithCounts().put(enumHashValuesMap.get(hash), hashCount.getValue());
            }
        }
    }
}
