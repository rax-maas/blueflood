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

import com.codahale.metrics.Timer;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.BooleanSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.shallows.EmptyColumnList;
import com.netflix.astyanax.util.RangeBuilder;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.exceptions.CacheException;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.ShardStateManager;
import com.rackspacecloud.blueflood.service.UpdateStamp;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class AstyanaxReader extends AstyanaxIO {
    private static final Logger log = LoggerFactory.getLogger(AstyanaxReader.class);
    private static final MetadataCache metaCache = MetadataCache.getInstance();
    private static final AstyanaxReader INSTANCE = new AstyanaxReader();

    private static final Keyspace keyspace = getKeyspace();
    private static final String UNKNOWN_UNIT = "unknown";

    public static AstyanaxReader getInstance() {
        return INSTANCE;
    }

    public Map<String, Object> getMetadataValues(Locator locator) throws ConnectionException {
        Timer.Context ctx = Instrumentation.getReadTimerContext(CassandraModel.CF_METRIC_METADATA);
        try {
            final ColumnList<String> results = keyspace.prepareQuery(CassandraModel.CF_METRIC_METADATA)
                    .getKey(locator)
                    .execute().getResult();
            return new HashMap<String, Object>(){{
                for (Column<String> result : results) {
                    put(result.getName(), result.getValue(MetadataSerializer.get()));
                }
            }};
        } catch (NotFoundException ex) {
            Instrumentation.markNotFound(CassandraModel.CF_METRIC_METADATA);
            return null;
        } catch (ConnectionException e) {
            log.error("Error reading metadata value", e);
            Instrumentation.markReadError(e);
            throw e;
        } finally {
            ctx.stop();
        }
    }

    // todo: this could be the basis for every rollup read method.
    // todo: A better interface may be to pass the serializer in instead of the class type.
    public <T extends Rollup> Points<T> getDataToRoll(Class<T> type, Locator locator, Range range, ColumnFamily<Locator, Long> cf) throws IOException {
        AbstractSerializer serializer = NumericSerializer.serializerFor(type);
        // special cases. :( the problem here is that the normal full res serializer returns Number instances instead of
        // SimpleNumber instances.
        // todo: this logic will only become more complicated. It needs to be in its own method and the serializer needs
        // to be known before we ever get to this method (see above comment).
        if (cf == CassandraModel.CF_METRICS_FULL)
            serializer = NumericSerializer.simpleNumberSerializer;
        else if ( cf == CassandraModel.CF_METRICS_PREAGGREGATED_FULL)
            serializer = type.equals(TimerRollup.class) ? NumericSerializer.timerRollupInstance : NumericSerializer.simpleNumberSerializer;
        
        ColumnList<Long> cols = getColumnsFromDB(locator, cf, range);
        Points<T> points = new Points<T>();
        try {
            for (Column<Long> col : cols) {
                points.add(new Points.Point<T>(col.getName(), (T)col.getValue(serializer)));
            }
        } catch (RuntimeException ex) {
            log.error("Problem deserializing data for " + locator + " (" + range + ") from " + cf.getName(), ex);
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
        Timer.Context ctx = Instrumentation.getReadTimerContext(CassandraModel.CF_METRICS_STATE);
        try {
            for (int shard : shards) {
                RowQuery<Long, String> query = keyspace
                        .prepareQuery(CassandraModel.CF_METRICS_STATE)
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

    private ColumnList<Long> getColumnsFromDB(Locator locator, ColumnFamily<Locator, Long> srcCF, Range range) {
        if (range.getStart() > range.getStop()) {
            throw new RuntimeException(String.format("Invalid rollup range: ", range.toString()));
        }

        final RangeBuilder rangeBuilder = new RangeBuilder().setStart(range.getStart()).setEnd(range.getStop());
        ColumnList<Long> columns;
        Timer.Context ctx = Instrumentation.getReadTimerContext(srcCF);
        try {
            RowQuery<Locator, Long> query = keyspace
                    .prepareQuery(srcCF)
                    .getKey(locator)
                    .withColumnRange(rangeBuilder.build());
            columns = query.execute().getResult();
            return columns;
        } catch (NotFoundException e) {
            Instrumentation.markNotFound(srcCF);
            return new EmptyColumnList<Long>();
        } catch (ConnectionException e) {
            Instrumentation.markReadError(e);
            log.error("Error getting data for " + locator + " (" + range + ") from " + srcCF.getName(), e);
            throw new RuntimeException("Error reading rollups", e);
        } finally {
            ctx.stop();
        }
    }

    public ColumnList<Locator> getAllLocators(long shard) {
        Timer.Context ctx = Instrumentation.getReadTimerContext(CassandraModel.CF_METRICS_LOCATOR);
        try {
            RowQuery<Long, Locator> query = keyspace
                    .prepareQuery(CassandraModel.CF_METRICS_LOCATOR)
                    .getKey(shard);
            return query.execute().getResult();
        } catch (NotFoundException e) {
            Instrumentation.markNotFound(CassandraModel.CF_METRICS_LOCATOR);
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

    public MetricData getHistogramsForRange(Locator locator, Range range, Granularity granularity) throws IOException {
        if (!granularity.isCoarser(Granularity.FULL)) {
            throw new RuntimeException("Histograms are not available for granularity " + granularity.toString());
        }

        ColumnFamily cf = CassandraModel.getColumnFamily(HistogramRollup.class, granularity);
        Points<HistogramRollup> histogramRollupPoints = getDataToRoll(HistogramRollup.class, locator, range, cf);
        return new MetricData(histogramRollupPoints, getUnitString(locator), MetricData.Type.HISTOGRAM);
    }

    // Used for string metrics
    private MetricData getStringMetricDataForRange(Locator locator, Range range, Granularity gran) {
        Points<String> points = new Points<String>();
        ColumnList<Long> results = getColumnsFromDB(locator, CassandraModel.CF_METRICS_STRING, range);

        for (Column<Long> column : results) {
            try {
                points.add(new Points.Point<String>(column.getName(), column.getValue(StringSerializer.get())));
            } catch (RuntimeException ex) {
                log.error("Problem deserializing String data for " + locator + " (" + range + ") from " +
                        CassandraModel.CF_METRICS_STRING.getName(), ex);
            }
        }

        return new MetricData(points, getUnitString(locator), MetricData.Type.STRING);
    }

    private MetricData getBooleanMetricDataForRange(Locator locator, Range range, Granularity gran) {
        Points<Boolean> points = new Points<Boolean>();
        ColumnList<Long> results = getColumnsFromDB(locator, CassandraModel.CF_METRICS_STRING, range);

        for (Column<Long> column : results) {
            try {
                points.add(new Points.Point<Boolean>(column.getName(), column.getValue(BooleanSerializer.get())));
            } catch (RuntimeException ex) {
                log.error("Problem deserializing Boolean data for " + locator + " (" + range + ") from " +
                        CassandraModel.CF_METRICS_STRING.getName(), ex);
            }
        }

        return new MetricData(points, getUnitString(locator), MetricData.Type.BOOLEAN);
    }

    // todo: replace this with methods that pertain to type (which can be used to derive a serializer).
    private MetricData getNumericMetricDataForRange(Locator locator, Range range, Granularity gran) {
        ColumnFamily<Locator, Long> CF = CassandraModel.getColumnFamily(BasicRollup.class, gran);

        Points<SimpleNumber> points = new Points<SimpleNumber>();
        ColumnList<Long> results = getColumnsFromDB(locator, CF, range);
        
        // todo: this will not work when we cannot derive data type from granularity. we will need to know what kind of
        // data we are asking for and use a specific reader method.
        AbstractSerializer serializer = gran == Granularity.FULL
                ? NumericSerializer.serializerFor(SimpleNumber.class)
                : NumericSerializer.serializerFor(BasicRollup.class);

        for (Column<Long> column : results) {
            try {
                points.add(pointFromColumn(column, gran, serializer));
            } catch (RuntimeException ex) {
                log.error("Problem deserializing data for " + locator + " (" + range + ") from " + CF.getName(), ex);
            }
        }

        return new MetricData(points, getUnitString(locator), MetricData.Type.NUMBER);
    }

    private MetricData getNumericOrStringRollupDataForRange(Locator locator, Range range, Granularity gran) {
        Instrumentation.markScanAllColumnFamilies();
        final ColumnFamily<Locator, Long> CF = CassandraModel.getColumnFamily(BasicRollup.class, gran);

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
        Timer.Context ctx = Instrumentation.getReadTimerContext(CassandraModel.CF_METRICS_STRING);

        try {
            ColumnList<Long> query = keyspace
                    .prepareQuery(CassandraModel.CF_METRICS_STRING)
                    .getKey(locator)
                    .withColumnRange(new RangeBuilder().setReversed(true).setLimit(1).build())
                    .execute()
                    .getResult();
            if (query.size() > 0) {
                metric = query.getColumnByIndex(0);
            }
        } catch (ConnectionException e) {
            if (e instanceof NotFoundException) {
                Instrumentation.markNotFound(CassandraModel.CF_METRICS_STRING);
            } else {
                Instrumentation.markReadError(e);
            }
            log.warn("Cannot get previous string metric value for locator " +
                    locator, e);
            throw e;
        } finally {
            ctx.stop();
        }

        return metric;
    }
}
