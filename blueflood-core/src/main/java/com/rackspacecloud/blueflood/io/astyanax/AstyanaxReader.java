/*
 * Copyright 2013 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.rackspacecloud.blueflood.io.astyanax;

import com.codahale.metrics.Timer;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Table;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.*;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.shallows.EmptyColumnList;
import com.netflix.astyanax.util.RangeBuilder;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.Instrumentation;
import com.rackspacecloud.blueflood.io.serializers.Serializers;
import com.rackspacecloud.blueflood.io.serializers.astyanax.StringMetadataSerializer;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class AstyanaxReader extends AstyanaxIO {
    private static final Logger log = LoggerFactory.getLogger(AstyanaxReader.class);
    private static final MetadataCache metaCache = MetadataCache.getInstance();
    private static final AstyanaxReader INSTANCE = new AstyanaxReader();
    private static final String rollupTypeCacheKey = MetricMetadata.ROLLUP_TYPE.toString().toLowerCase();
    private static final Keyspace keyspace = getKeyspace();

    public static AstyanaxReader getInstance() {
        return INSTANCE;
    }

    /**
     * Method that returns all metadata for a given locator as a map.
     *
     * @param locator  locator name
     * @return Map of metadata for that locator
     * @throws RuntimeException(com.netflix.astyanax.connectionpool.exceptions.ConnectionException)
     */
    public Map<String, String> getMetadataValues(Locator locator) {
        Timer.Context ctx = Instrumentation.getReadTimerContext(CassandraModel.CF_METRICS_METADATA_NAME);
        try {
            final ColumnList<String> results = keyspace.prepareQuery(CassandraModel.CF_METRICS_METADATA)
                    .getKey(locator)
                    .execute().getResult();
            return new HashMap<String, String>(){{
                for (Column<String> result : results) {
                    put(result.getName(), result.getValue(StringMetadataSerializer.get()));
                }
            }};
        } catch (NotFoundException ex) {
            Instrumentation.markNotFound(CassandraModel.CF_METRICS_METADATA_NAME);
            return null;
        } catch (ConnectionException e) {
            log.error("Error reading metadata value", e);
            Instrumentation.markReadError(e);
            throw new RuntimeException(e);
        } finally {
            ctx.stop();
        }
    }

    public Table<Locator, String, String> getMetadataValues(Set<Locator> locators) {
        ColumnFamily CF = CassandraModel.CF_METRICS_METADATA;
        boolean isBatch = locators.size() > 1;
        Table<Locator, String, String> metaTable = HashBasedTable.create();

        Timer.Context ctx = isBatch ? Instrumentation.getBatchReadTimerContext(CF.getName()) : Instrumentation.getReadTimerContext(CF.getName());
        try {
            // We don't paginate this call. So we should make sure the number of reads is tolerable.
            // TODO: Think about paginating this call.
            OperationResult<Rows<Locator, String>> query = keyspace
                    .prepareQuery(CF)
                    .getKeySlice(locators)
                    .execute();

            for (Row<Locator, String> row : query.getResult()) {
                ColumnList<String> columns = row.getColumns();
                for (Column<String> column : columns) {
                    String metaValue = column.getValue(StringMetadataSerializer.get());
                    String metaKey = column.getName();
                    metaTable.put(row.getKey(), metaKey, metaValue);
                }
            }
        } catch (ConnectionException e) {
            if (e instanceof NotFoundException) { // TODO: Not really sure what happens when one of the keys is not found.
                Instrumentation.markNotFound(CF.getName());
            } else {
                if (isBatch) { Instrumentation.markBatchReadError(e); }
                else { Instrumentation.markReadError(e); }
            }
            log.error((isBatch ? "Batch " : "") + " read query failed for column family " + CF.getName() + " for locators: " + StringUtils.join(locators, ","), e);
        } finally {
            ctx.stop();
        }

        return metaTable;
    }

    private ColumnList<Long> getColumnsFromDB(final Locator locator, ColumnFamily<Locator, Long> srcCF, Range range) {
        List<Locator> locators = new LinkedList<Locator>(){{ add(locator); }};
        ColumnList<Long> columns = getColumnsFromDB(locators, srcCF, range).get(locator);
        return columns == null ? new EmptyColumnList<Long>() : columns;
    }

    protected Map<Locator, ColumnList<Long>> getColumnsFromDB(List<Locator> locators, ColumnFamily<Locator, Long> CF,
                                                            Range range) {
        if (range.getStart() > range.getStop()) {
            throw new RuntimeException(String.format("Invalid rollup range: ", range.toString()));
        }
        boolean isBatch = locators.size() != 1;

        final Map<Locator, ColumnList<Long>> columns = new HashMap<Locator, ColumnList<Long>>();
        final RangeBuilder rangeBuilder = new RangeBuilder().setStart(range.getStart()).setEnd(range.getStop());

        Timer.Context ctx = isBatch ? Instrumentation.getBatchReadTimerContext(CF.getName()) : Instrumentation.getReadTimerContext(CF.getName());
        try {
            // We don't paginate this call. So we should make sure the number of reads is tolerable.
            // TODO: Think about paginating this call.
            OperationResult<Rows<Locator, Long>> query = keyspace
                    .prepareQuery(CF)
                    .getKeySlice(locators)
                    .withColumnRange(rangeBuilder.build())
                    .execute();
            for (Row<Locator, Long> row : query.getResult()) {
                columns.put(row.getKey(), row.getColumns());
            }

        } catch (ConnectionException e) {
            if (e instanceof NotFoundException) { // TODO: Not really sure what happens when one of the keys is not found.
                Instrumentation.markNotFound(CF.getName());
            } else {
                if (isBatch) { Instrumentation.markBatchReadError(e); }
                else { Instrumentation.markReadError(e); }
            }
            log.error((isBatch ? "Batch " : "") + " read query failed for column family " + CF.getName() + " for locators: " + StringUtils.join(locators, ","), e);
        } finally {
            ctx.stop();
        }

        return columns;
    }

    // todo: this could be the basis for every rollup read method.
    // todo: A better interface may be to pass the serializer in instead of the class type.
    public <T extends Rollup> Points<T> getDataToRoll(Class<T> type, final Locator locator, Range range, ColumnFamily<Locator, Long> cf) throws IOException {
        AbstractSerializer serializer = Serializers.serializerFor(type);
        // special cases. :( the problem here is that the normal full res serializer returns Number instances instead of
        // SimpleNumber instances.
        // todo: this logic will only become more complicated. It needs to be in its own method and the serializer needs
        // to be known before we ever get to this method (see above comment).
        if (cf == CassandraModel.CF_METRICS_FULL) {
            serializer = Serializers.simpleNumberSerializer;
        } else if ( cf == CassandraModel.CF_METRICS_PREAGGREGATED_FULL) {
            // consider a method for this.  getSerializer(CF, TYPE);
            if (type.equals(BluefloodTimerRollup.class)) {
                serializer = Serializers.timerRollupInstance;
            } else if (type.equals(BluefloodSetRollup.class)) {
                serializer = Serializers.setRollupInstance;
            } else if (type.equals(BluefloodGaugeRollup.class)) {
                serializer = Serializers.gaugeRollupInstance;
            } else if (type.equals(BluefloodCounterRollup.class)) {
                serializer = Serializers.counterRollupInstance;
            }
            else {
                serializer = Serializers.simpleNumberSerializer;
            }
        }

        ColumnList<Long> cols = getColumnsFromDB(locator, cf, range);
        Points<T> points = new Points<T>();
        try {
            for (Column<Long> col : cols) {
                points.add(new Points.Point<T>(col.getName(), (T)col.getValue(serializer)));
            }

            // we only want to count the number of points we
            // get when we're querying the metrics_full
            // we don't do for aggregated or other granularities
            if ( cf == CassandraModel.CF_METRICS_FULL ) {
                Instrumentation.getRawPointsIn5MinHistogram().update(points.getPoints().size());
            }
        } catch (RuntimeException ex) {
            log.error("Problem deserializing data for " + locator + " (" + range + ") from " + cf.getName(), ex);
            throw new IOException(ex);
        }
        return points;
    }

    /**
     * Get data points for a particular {@link Locator}, {@link Range}, and
     * {@link Granularity}.
     *
     * This method is eventually called from Rollup process.
     *
     * @param locator
     * @param range
     * @param gran
     * @return a {@link MetricData} containing the data points requested
     */
    public MetricData getDatapointsForRange(Locator locator, Range range, Granularity gran) {
        RollupType rollupType = RollupType.BF_BASIC;
        String rollupTypeStr = metaCache.safeGet(locator, rollupTypeCacheKey);
        if ( rollupTypeStr != null ) {
            rollupType = RollupType.fromString(rollupTypeStr);
        }

        return getNumericMetricDataForRange(locator, range, gran, rollupType);
    }

    /**
     * Get data points for multiple {@link Locator}, for the specified {@link Range} and
     * {@link Granularity}.
     *
     * This method is eventually called from all the output handlers for query requests.
     *
     * @param locators
     * @param range
     * @param gran
     * @return
     */
    public Map<Locator, MetricData> getDatapointsForRange(List<Locator> locators, Range range, Granularity gran) {
        ListMultimap<ColumnFamily, Locator> locatorsByCF =
                ArrayListMultimap.create();
        Map<Locator, MetricData> results = new HashMap<Locator, MetricData>();
        for (Locator locator : locators) {
            try {
                RollupType rollupType = RollupType.fromString(
                        metaCache.get(locator, MetricMetadata.ROLLUP_TYPE.name().toLowerCase()));

                ColumnFamily cf = CassandraModel.getColumnFamily(rollupType, gran);
                List<Locator> locs = locatorsByCF.get(cf);
                locs.add(locator);
            } catch (Exception e) {
                // pass for now. need metric to figure this stuff out.
                log.error(String.format("error getting datapoints for locator %s, range %s, granularity %s", locator, range.toString(), gran.toString()), e);
            }
        }

        for (ColumnFamily CF : locatorsByCF.keySet()) {
            List<Locator> locs = locatorsByCF.get(CF);
            results.putAll(getNumericDataForRangeLocatorList(range, gran, CF, locs));
        }

        return results;
    }

    private Map<Locator, MetricData> getNumericDataForRangeLocatorList(Range range, Granularity gran, ColumnFamily CF, List<Locator> locs) {
        Map<Locator, ColumnList<Long>> metrics = getColumnsFromDB(locs, CF, range);
        Map<Locator, MetricData> results = new HashMap<Locator, MetricData>();

        // transform columns to MetricData
        for (Locator loc : metrics.keySet()) {
            MetricData data = transformColumnsToMetricData(loc, metrics.get(loc), gran);
            if (data != null && data.getData() != null) {
                results.put(loc, data);
            }
        }

        return results;
    }

    private MetricData getNumericMetricDataForRange(Locator locator, Range range, Granularity gran, RollupType rollupType) {
        ColumnFamily<Locator, Long> CF = CassandraModel.getColumnFamily(rollupType, gran);
        Points points = new Points();
        ColumnList<Long> results = getColumnsFromDB(locator, CF, range);

        // todo: this will not work when we cannot derive data type from granularity. we will need to know what kind of
        // data we are asking for and use a specific reader method.
        AbstractSerializer serializer = Serializers.serializerFor(RollupType.classOf(rollupType, gran));

        for (Column<Long> column : results) {
            try {
                points.add(pointFromColumn(column, serializer));
            } catch (RuntimeException ex) {
                log.error("Problem deserializing data for " + locator + " (" + range + ") from " + CF.getName(), ex);
            }
        }

        return new MetricData(points, metaCache.getUnitString(locator));
    }

    private MetricData transformColumnsToMetricData(Locator locator, ColumnList<Long> columns,
                                                    Granularity gran) {
        try {
            RollupType rollupType = RollupType.fromString(metaCache.get(locator, rollupTypeCacheKey));
            String unit = metaCache.getUnitString(locator);
            Points points = getPointsFromColumns(columns, rollupType, gran);
            MetricData data = new MetricData(points, unit);
            return data;
        } catch (Exception e) {
            return null;
        }
    }

    private Points getPointsFromColumns(ColumnList<Long> columnList, RollupType rollupType,
                                        Granularity gran) {
        Points points = new Points();

        AbstractSerializer serializer = serializerFor(rollupType, gran);
        for (Column<Long> column : columnList) {
            points.add(pointFromColumn(column, serializer));
        }

        return points;
    }

    private Points.Point pointFromColumn(Column<Long> column, AbstractSerializer serializer) {
        if (serializer instanceof Serializers.RawSerializer) {
            return new Points.Point(column.getName(), new SimpleNumber(column.getValue(serializer)));
        }
        else
            // this works for EVERYTHING except SimpleNumber.
            return new Points.Point(column.getName(), column.getValue(serializer));
    }
}
