package com.cloudkick.blueflood.io;

import com.cloudkick.blueflood.cache.MetadataCache;
import com.cloudkick.blueflood.exceptions.CacheException;
import com.cloudkick.blueflood.outputs.formats.RollupData;
import com.cloudkick.blueflood.rollup.Granularity;
import com.cloudkick.blueflood.service.ShardStateManager;
import com.cloudkick.blueflood.service.UpdateStamp;
import com.cloudkick.blueflood.types.*;
import com.cloudkick.blueflood.utils.Util;
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
import telescope.thrift.MetricInfo;
import telescope.thrift.UnitEnum;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class AstyanaxReader extends AstyanaxIO {
    private static final Logger log = LoggerFactory.getLogger(AstyanaxReader.class);

    private static final String GET_ALL_SHARDS = "Get all shards";
    private static final String GET_NUMERIC_ROLLUPS = "Get Numeric Rollup Iterator";
    private static final String GET_STRING_METRICS = "Get String Rollup Iterator";
    private static final String LOCATOR_ITERATOR = "Get Locators Iterator";
    private static final String GET_LAST_METRICS_VALUE = "Get last metric value";
    private static final String GET_METRICS_FOR_CHECK = "Get list of metrics for check";
    private static final String GET_METADATA = "Get Metadata col";
    private static final MetadataCache metaCache = MetadataCache.getInstance();
    private static final AstyanaxReader INSTANCE = new AstyanaxReader();

    private static final Keyspace keyspace = getKeyspace();

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
            Instrumentation.markReadError();
            throw e;
        } finally {
            ctx.stop();
        }
    }

    /**
     * reads a column slice and calculates an average. this method doesn't do any checking to ensure that the range
     * specified is over a single slot.
     * @param locator
     * @param range
     * @param gran
     * @return
     */
    public Rollup readAndCalculate(Locator locator, Range range, Granularity gran) {
        Rollup rollup = new Rollup();

        NumericSerializer serializer = NumericSerializer.get(gran);
        ColumnList<Long> cols = getNumericRollups(locator, gran, range.start, range.stop);
        for (Column col : cols) {
            try {
                if (gran == Granularity.FULL) {
                    rollup.handleFullResMetric(col.getValue(serializer));
                } else {
                    rollup.handleRollupMetric((Rollup)col.getValue(serializer));
                }
            } catch (IOException ex) {
                log.error("Problem deserializing data at granularity: " + gran.name() + ".");
                log.error(ex.getMessage(), ex);
            }
        }

        return rollup;
    }

    public static String getUnitString(Locator locator) {
        String unitString = null;
        try {
            unitString = metaCache.get(locator, MetricMetadata.UNIT.name().toLowerCase(), String.class);
        } catch (CacheException ex) {
            log.warn("Cache exception reading unitString from MetadataCache: ", ex);
        }
        if (unitString == null) {
            unitString = UnitEnum.UNKNOWN.toString().toLowerCase();
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
            Instrumentation.markReadError();
            log.error("Error getting all shard states", e);
            throw e;
        } finally {
            ctx.stop();
        }
    }

    public List<MetricInfo> getMetricsForCheck(String accountId, String entityId, String checkId) {
        final List<MetricInfo> results = new ArrayList<MetricInfo>();

        final String dBKey = Util.generateMetricsDiscoveryDBKey(accountId, entityId, checkId);
        for (Column<String> col : getMetricsList(dBKey)) {
            String metric = col.getName();
            String unitString = getUnitString(Locator.createLocatorFromPathComponents(accountId, entityId, checkId, metric));
            results.add(new MetricInfo(col.getName(), unitString));
        }
        return results;
    }

    private ColumnList<String> getMetricsList(final String dBKey) {
        TimerContext ctx = Instrumentation.getTimerContext(GET_METRICS_FOR_CHECK);
        try {
            RowQuery<String, String> query = keyspace
                    .prepareQuery(CF_METRICS_DISCOVERY)
                    .getKey(dBKey);
            return query.execute().getResult();
        } catch (NotFoundException e) {
            return new EmptyColumnList<String>();
        } catch (ConnectionException e) {
            Instrumentation.markReadError();
            log.error("Error getting metrics list", e);
            throw new RuntimeException("Error getting metrics list", e);
        } finally {
            ctx.stop();
        }
    }

    public ColumnList<Long> getNumericRollups(Locator locator, Granularity granularity, long from, long to) {
        if (from > to) throw new RuntimeException(String.format("Invalid rollup period %d->%d", from, to));

        final RangeBuilder rangeBuilder = new RangeBuilder().setStart(from).setEnd(to);
        ColumnList<Long> columns;
        TimerContext ctx = Instrumentation.getTimerContext(GET_NUMERIC_ROLLUPS);
        try {
            RowQuery<Locator, Long> query = keyspace
                    .prepareQuery(CF_NAME_TO_CF.get(granularity.name()))
                    .getKey(locator)
                    .withColumnRange(rangeBuilder.build());
            columns = query.execute().getResult();
            return columns;
        } catch (NotFoundException e) {
            return new EmptyColumnList<Long>();
        } catch (ConnectionException e) {
            Instrumentation.markReadError();
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
            Instrumentation.markReadError();
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
            Instrumentation.markReadError();
            log.error("Error reading locators", e);
            throw new RuntimeException("Error reading locators", e);
        } finally {
            ctx.stop();
        }
    }

    public RollupData getDatapointsForRange(Locator locator, Range range, Granularity gran) {
        try {
            Object type = metaCache.get(locator, "type");

            if (type == null) {
                return scanAllColumnFamiliesForPoints(locator, range, gran);
            }

            com.cloudkick.blueflood.types.Metric.Type metricType = new com.cloudkick.blueflood.types.Metric.Type((String) type);
            if (!com.cloudkick.blueflood.types.Metric.Type.isKnownMetricType(metricType)) {
                return scanAllColumnFamiliesForPoints(locator, range, gran);
            }

            if (metricType.equals(com.cloudkick.blueflood.types.Metric.Type.STRING)) {
                return getStringDatapointsForRange(locator, range, gran, StringSerializer.get());
            } else if (metricType.equals(com.cloudkick.blueflood.types.Metric.Type.BOOLEAN)) {
                return getStringDatapointsForRange(locator, range, gran, BooleanSerializer.get());
            } else {
                return getNumericDatapointsForRange(locator, range, gran);
            }

        } catch (CacheException e) {
            log.warn("Caught exception trying to find metric type from meta cache for locator " + locator.toString(), e);
            return scanAllColumnFamiliesForPoints(locator, range, gran);
        }
    }

    // Used for both string and boolean metrics
    private RollupData getStringDatapointsForRange(Locator locator, Range range, Granularity gran, AbstractSerializer serializer) {
        Points points =  transformDBValuesToPoints(
                getStringPoints(locator, range.start, range.stop), gran, serializer);
        return new RollupData(points, getUnitString(locator));
    }

    private RollupData getNumericDatapointsForRange(Locator locator, Range range, Granularity gran) {
        Points points = transformDBValuesToPoints(
                getNumericRollups(locator, gran, range.start, range.stop), gran, NumericSerializer.get(gran));
        return new RollupData(points, getUnitString(locator));
    }

    private RollupData scanAllColumnFamiliesForPoints(Locator locator, Range range, Granularity gran) {
        Instrumentation.markScanAllColumnFamilies();
        ColumnList<Long> results = getNumericRollups(locator, gran, range.getStart(), range.getStop());
        Points points = transformDBValuesToPoints(results, gran, NumericSerializer.get(gran));

        if (points.getPoints().size() > 0) {
            return new RollupData(points, getUnitString(locator));
        }

        results = getStringPoints(locator, range.start, range.stop);
        return new RollupData(transformDBValuesToPoints(results, gran, StringSerializer.get()), "");
    }

    private Points transformDBValuesToPoints(ColumnList<Long> results, Granularity gran, AbstractSerializer serializer) {
        Points points = Points.create(gran);

        for (Column<Long> column : results) {
            try {
                points.add(pointFromColumn(column, gran, serializer));
            } catch (RuntimeException ex) {
                log.error("Problem deserializing rollup"); // TODO: update message?
                log.error(ex.getMessage(), ex);
            }
        }

        return points;
    }

    private Points.Point pointFromColumn(Column<Long> column, Granularity gran, AbstractSerializer serializer) {
        if (gran == Granularity.FULL) {
            return new Points.Point<Object>(column.getName(), column.getValue(serializer));
        } else {
            Rollup rollup = (Rollup) column.getValue(serializer);
            return new Points.Point<Rollup>(column.getName(), rollup);
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
            Instrumentation.markReadError();
            log.warn("Cannot get previous string metric value for locator " +
                    locator, e);
            throw e;
        } finally {
            ctx.stop();
        }

        return metric;
    }
}
