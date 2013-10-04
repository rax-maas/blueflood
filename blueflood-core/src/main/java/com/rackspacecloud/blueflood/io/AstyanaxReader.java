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

package com.rackspacecloud.blueflood.io;

import com.netflix.astyanax.model.ColumnFamily;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.exceptions.CacheException;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.ShardStateManager;
import com.rackspacecloud.blueflood.service.UpdateStamp;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Util;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.BooleanSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.shallows.EmptyColumnList;
import com.netflix.astyanax.util.RangeBuilder;
import com.yammer.metrics.core.TimerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

public class AstyanaxReader extends AstyanaxIO {
    private static final Logger log = LoggerFactory.getLogger(AstyanaxReader.class);

    private static final String GET_ALL_SHARDS = "Get all shards";
    private static final String GET_NUMERIC_ROLLUPS = "Get Numeric Rollup Iterator";
    private static final String GET_STRING_METRICS = "Get String Rollup Iterator";
    private static final String LOCATOR_ITERATOR = "Get Locators Iterator";
    private static final String GET_LAST_METRICS_VALUE = "Get last metric value";
    private static final String GET_METADATA = "Get Metadata col";
    private static final MetadataCache metaCache = MetadataCache.getInstance();
    private static final AstyanaxReader INSTANCE = new AstyanaxReader();

    private static final Keyspace keyspace = getKeyspace();
    private static final String UNKNOWN_UNIT = "unknown";

    public static AstyanaxReader getInstance() {
        return INSTANCE;
    }

    public Object getMetadataValue(Locator locator, String metaKey) throws ConnectionException {
        TimerContext ctx = Instrumentation.getTimerContext(GET_METADATA);
        try {
            Column<String> result = keyspace.prepareQuery(CF_METRIC_METADATA)
                    .getKey(locator)
                    .getColumn(metaKey)
                    .execute().getResult();
            return result.getValue(MetadataSerializer.get());
        } catch (NotFoundException ex) {
            return null;
        } catch (ConnectionException e) {
            log.error("Error reading metadata value", e);
            Instrumentation.markReadError(e);
            throw e;
        } finally {
            ctx.stop();
        }
    }

    /**
     * reads a column slice and returns data points. this method doesn't do any checking to ensure that the range
     * specified is over a single slot.
     * @param locator
     * @param range
     * @param srcCF
     * @return
     */
    public Points<BasicRollup> getBasicRollupDataToRoll(Locator locator, Range range, ColumnFamily<Locator, Long> srcCF) throws IOException {
        if (srcCF.equals(AstyanaxIO.CF_METRICS_FULL))
            throw new IOException("This method should not be used for full resolution data");
        
        AbstractSerializer serializer = NumericSerializer.get(srcCF);
        ColumnList<Long> cols = getNumericRollups(locator, srcCF, range.start, range.stop);
        Points<BasicRollup> points = new Points<BasicRollup>();
        
        try {
            for (Column<Long> col : cols) {
                points.add(new Points.Point<BasicRollup>(col.getName(), (BasicRollup) col.getValue(serializer)));
            }
        } catch (RuntimeException ex) {
            log.error("Problem deserializing data", ex);
            throw new IOException(ex);
        }

        return points;    
    }
    
    public Points<SimpleNumber> getSimpleDataToRoll(Locator locator, Range range) throws IOException {
        AbstractSerializer serializer = NumericSerializer.get(AstyanaxIO.CF_METRICS_FULL);
        ColumnList<Long> cols = getNumericRollups(locator, AstyanaxIO.CF_METRICS_FULL, range.start, range.stop);
        Points<SimpleNumber> points =new Points<SimpleNumber>();
        
        try {
            for (Column<Long> col : cols) {
                points.add(new Points.Point<SimpleNumber>(col.getName(), new SimpleNumber(col.getValue(serializer))));
            }
        } catch (RuntimeException ex) {
            log.error("Problem deserializing data", ex);
            throw new IOException(ex);
        }
        
        return points;
    }

    public static String getUnitString(Locator locator) {
        String unitString = null;
        try {
            unitString = metaCache.get(locator, MetricMetadata.UNIT.name().toLowerCase(), String.class);
        } catch (CacheException ex) {
            log.warn("Cache exception reading unitString from MetadataCache: ", ex);
        }
        if (unitString == null) {
            unitString = UNKNOWN_UNIT;
        }
        return unitString;
    }

    public void getAndUpdateAllShardStates(ShardStateManager shardStateManager, Collection<Integer> shards) throws ConnectionException {
        TimerContext ctx = Instrumentation.getTimerContext(GET_ALL_SHARDS);
        try {
            for (int shard : shards) {
                RowQuery<Long, String> query = keyspace
                        .prepareQuery(CF_METRICS_STATE)
                        .getKey((long) shard);
                ColumnList<String> columns = query.execute().getResult();

                for (Column<String> column : columns) {
                    String columnName = column.getName();
                    long timestamp = column.getLongValue();
                    Granularity g = Util.granularityFromStateCol(columnName);
                    int slot = Util.slotFromStateCol(columnName);

                    boolean isRemove = UpdateStamp.State.Rolled.code().equals(Util.stateFromStateCol(columnName));
                    UpdateStamp.State state = isRemove ? UpdateStamp.State.Rolled : UpdateStamp.State.Active;

                    shardStateManager.updateSlotOnRead(shard, g, slot, timestamp, state);
                }
            }
        } catch (ConnectionException e) {
            Instrumentation.markReadError(e);
            log.error("Error getting all shard states", e);
            throw e;
        } finally {
            ctx.stop();
        }
    }

    public ColumnList<Long> getNumericRollups(Locator locator, ColumnFamily<Locator, Long> srcCF, long from, long to) {
        if (from > to) throw new RuntimeException(String.format("Invalid rollup period %d->%d", from, to));

        final RangeBuilder rangeBuilder = new RangeBuilder().setStart(from).setEnd(to);
        ColumnList<Long> columns;
        TimerContext ctx = Instrumentation.getTimerContext(GET_NUMERIC_ROLLUPS);
        try {
            RowQuery<Locator, Long> query = keyspace
                    .prepareQuery(srcCF)
                    .getKey(locator)
                    .withColumnRange(rangeBuilder.build());
            columns = query.execute().getResult();
            return columns;
        } catch (NotFoundException e) {
            return new EmptyColumnList<Long>();
        } catch (ConnectionException e) {
            Instrumentation.markReadError(e);
            log.error("Error getting numeric rollups", e);
            throw new RuntimeException("Error reading numeric rollups", e);
        } finally {
            ctx.stop();
        }
    }

    public ColumnList<Long> getStringPoints(final Locator locator, final long from, final long to) {
        if (from > to) throw new IllegalArgumentException(String.format("Invalid rollup period %d->%d", from, to));

        final RangeBuilder rangeBuilder = new RangeBuilder().setStart(from).setEnd(to);
        ColumnList<Long> columns;
        TimerContext ctx = Instrumentation.getTimerContext(GET_STRING_METRICS);
        try {
            RowQuery<Locator, Long> query = keyspace
                    .prepareQuery(CF_METRICS_STRING)
                    .getKey(locator)
                    .withColumnRange(rangeBuilder.build());
            columns = query.execute().getResult();
            return columns;
        } catch (NotFoundException e) {
            Instrumentation.markStringsNotFound();
            return new EmptyColumnList<Long>();
        } catch (ConnectionException e) {
            Instrumentation.markReadError(e);
            log.error("Error reading string points", e);
            throw new RuntimeException("Error reading string points", e);
        } finally {
            ctx.stop();
        }
    }

    public ColumnList<Locator> getAllLocators(long shard) {
        TimerContext ctx = Instrumentation.getTimerContext(LOCATOR_ITERATOR);
        try {
            RowQuery<Long, Locator> query = keyspace
                    .prepareQuery(CF_METRICS_LOCATOR)
                    .getKey(shard);
            return query.execute().getResult();
        } catch (NotFoundException e) {
            return new EmptyColumnList<Locator>();
        } catch (ConnectionException e) {
            Instrumentation.markReadError(e);
            log.error("Error reading locators", e);
            throw new RuntimeException("Error reading locators", e);
        } finally {
            ctx.stop();
        }
    }

    public MetricData getDatapointsForRange(Locator locator, Range range, Granularity gran) {
        try {
            Object type = metaCache.get(locator, "type");

            if (type == null) {
                return getNumericOrStringRollupDataForRange(locator, range, gran);
            }

            Metric.Type metricType = new Metric.Type((String) type);
            if (!com.rackspacecloud.blueflood.types.Metric.Type.isKnownMetricType(metricType)) {
                return getNumericOrStringRollupDataForRange(locator, range, gran);
            }

            if (metricType.equals(com.rackspacecloud.blueflood.types.Metric.Type.STRING)) {
                gran = Granularity.FULL;
                return getStringMetricDataForRange(locator, range, gran);
            } else if (metricType.equals(com.rackspacecloud.blueflood.types.Metric.Type.BOOLEAN)) {
                gran = Granularity.FULL;
                return getBooleanMetricDataForRange(locator, range, gran);
            } else {
                return getNumericMetricDataForRange(locator, range, gran);
            }

        } catch (CacheException e) {
            log.warn("Caught exception trying to find metric type from meta cache for locator " + locator.toString(), e);
            return getNumericOrStringRollupDataForRange(locator, range, gran);
        }
    }

    // Used for string metrics
    private MetricData getStringMetricDataForRange(Locator locator, Range range, Granularity gran) {
        Points<String> points = new Points<String>();
        ColumnList<Long> results = getStringPoints(locator, range.start, range.stop);

        for (Column<Long> column : results) {
            try {
                points.add(new Points.Point<String>(column.getName(), column.getValue(StringSerializer.get())));
            } catch (RuntimeException ex) {
                log.error("Problem deserializing rollup"); // TODO: update message?
                log.error(ex.getMessage(), ex);
            }
        }

        return new MetricData(points, getUnitString(locator));
    }

    private MetricData getBooleanMetricDataForRange(Locator locator, Range range, Granularity gran) {
        Points<Boolean> points = new Points<Boolean>();
        ColumnList<Long> results = getStringPoints(locator, range.start, range.stop);

        for (Column<Long> column : results) {
            try {
                points.add(new Points.Point<Boolean>(column.getName(), column.getValue(BooleanSerializer.get())));
            } catch (RuntimeException ex) {
                log.error("Problem deserializing rollup"); // TODO: update message?
                log.error(ex.getMessage(), ex);
            }
        }

        return new MetricData(points, getUnitString(locator));
    }

    private MetricData getNumericMetricDataForRange(Locator locator, Range range, Granularity gran) {
        ColumnFamily<Locator, Long> CF = AstyanaxIO.CF_NAME_TO_CF.get(gran.name());

        Points<SimpleNumber> points = new Points<SimpleNumber>();
        ColumnList<Long> results = getNumericRollups(locator, AstyanaxIO.CF_NAME_TO_CF.get(gran.name()),
                range.start, range.stop);

        for (Column<Long> column : results) {
            try {
                points.add(pointFromColumn(column, gran, NumericSerializer.get(CF)));
            } catch (RuntimeException ex) {
                log.error("Problem deserializing rollup"); // TODO: update message?
                log.error(ex.getMessage(), ex);
            }
        }

        return new MetricData(points, getUnitString(locator));
    }

    private MetricData getNumericOrStringRollupDataForRange(Locator locator, Range range, Granularity gran) {
        Instrumentation.markScanAllColumnFamilies();
        final ColumnFamily<Locator, Long> CF = AstyanaxIO.CF_NAME_TO_CF.get(gran.name());

        final MetricData metricData = getNumericMetricDataForRange(locator, range, gran);

        if (metricData.getData().getPoints().size() > 0) {
            return metricData;
        }

        return getStringMetricDataForRange(locator, range, gran);
    }

    private Points.Point pointFromColumn(Column<Long> column, Granularity gran, AbstractSerializer serializer) {
        if (gran == Granularity.FULL) {
            return new Points.Point<SimpleNumber>(column.getName(), new SimpleNumber(column.getValue(serializer)));
        } else {
            BasicRollup basicRollup = (BasicRollup) column.getValue(serializer);
            return new Points.Point<BasicRollup>(column.getName(), basicRollup);
        }
    }

    /**
     * Method that makes the actual cassandra call to get last string metric for a locator
     *
     * @param locator  locator name
     * @return Set of previous metrics in database
     * @throws Exception
     */
    public Column<Long> getLastMetricFromMetricsString(Locator locator)
            throws Exception {
        Column<Long> metric = null;
        TimerContext ctx = Instrumentation.getTimerContext(GET_LAST_METRICS_VALUE);

        try {
            ColumnList<Long> query = keyspace
                    .prepareQuery(CF_METRICS_STRING)
                    .getKey(locator)
                    .withColumnRange(new RangeBuilder().setReversed(true).setLimit(1).build())
                    .execute()
                    .getResult();
            if (query.size() > 0) {
                metric = query.getColumnByIndex(0);
            }
        } catch (ConnectionException e) {
            Instrumentation.markReadError(e);
            log.warn("Cannot get previous string metric value for locator " +
                    locator, e);
            throw e;
        } finally {
            ctx.stop();
        }

        return metric;
    }
}
