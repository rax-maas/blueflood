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
import com.netflix.astyanax.connectionpool.exceptions.PoolTimeoutException;
import com.netflix.astyanax.model.*;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.BooleanSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.shallows.EmptyColumnList;
import com.netflix.astyanax.util.RangeBuilder;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.exceptions.CacheException;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.Instrumentation;
import com.rackspacecloud.blueflood.io.serializers.Serializers;
import com.rackspacecloud.blueflood.io.serializers.astyanax.StringMetadataSerializer;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Util;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class AstyanaxReader extends AstyanaxIO {
    private static final Logger log = LoggerFactory.getLogger(AstyanaxReader.class);
    private static final MetadataCache metaCache = MetadataCache.getInstance();
    private static final AstyanaxReader INSTANCE = new AstyanaxReader();
    private static final String rollupTypeCacheKey = MetricMetadata.ROLLUP_TYPE.toString().toLowerCase();
    private static final String dataTypeCacheKey = MetricMetadata.TYPE.toString().toLowerCase();

    private static final Keyspace keyspace = getKeyspace();
    private static int enumThreadCount = Configuration.getInstance().getIntegerProperty(CoreConfig.ENUM_READ_THREADS);
    private ExecutorService taskExecutor =  null;

    public static AstyanaxReader getInstance() {
        if (INSTANCE.taskExecutor == null || INSTANCE.taskExecutor.isShutdown()) {
            INSTANCE.taskExecutor = new ThreadPoolBuilder().withUnboundedQueue()
                    .withCorePoolSize(enumThreadCount)
                    .withMaxPoolSize(enumThreadCount).withName("Retrieving Enum Values").build();
        }
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

    /**
     * Method that makes the actual cassandra call to get the most recent string value for a locator
     *
     * @param locator  locator name
     * @return String most recent string value for metric.
     * @throws RuntimeException(com.netflix.astyanax.connectionpool.exceptions.ConnectionException)
     */
    public String getLastStringValue(Locator locator) {
        Timer.Context ctx = Instrumentation.getReadTimerContext(CassandraModel.CF_METRICS_STRING_NAME);

        try {
            ColumnList<Long> query = keyspace
                    .prepareQuery(CassandraModel.CF_METRICS_STRING)
                    .getKey(locator)
                    .withColumnRange(new RangeBuilder().setReversed(true).setLimit(1).build())
                    .execute()
                    .getResult();

            return query.isEmpty() ? null : query.getColumnByIndex(0).getStringValue();
        } catch (ConnectionException e) {
            if (e instanceof NotFoundException) {
                Instrumentation.markNotFound(CassandraModel.CF_METRICS_STRING_NAME);
            } else {
                Instrumentation.markReadError(e);
            }
            log.warn("Could not get previous string metric value for locator " +
                    locator, e);
            throw new RuntimeException(e);
        } finally {
            ctx.stop();
        }
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
            } else if (type.equals(BluefloodEnumRollup.class)) {
                serializer = Serializers.enumRollupInstance;
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
        } catch (RuntimeException ex) {
            log.error("Problem deserializing data for " + locator + " (" + range + ") from " + cf.getName(), ex);
            throw new IOException(ex);
        }
        return points;
    }

    public static String getType(Locator locator) {
        String type = null;
        try {
            type = metaCache.get(locator, MetricMetadata.TYPE.name().toLowerCase(), String.class);
        } catch (CacheException ex) {
            log.warn("Cache exception reading type from MetadataCache. ", ex);
        }
        if (type == null) {
            type = Util.UNKNOWN;
        }
        return type;
    }

    public MetricData getDatapointsForRange(Locator locator, Range range, Granularity gran) {
        try {
            Object type = metaCache.get(locator, dataTypeCacheKey);
            RollupType rollupType = RollupType.fromString(metaCache.get(locator, rollupTypeCacheKey));

            if (rollupType == null) {
                rollupType = RollupType.BF_BASIC;
            }
            if (rollupType == RollupType.ENUM){
                return getEnumMetricDataForRange(locator, range, gran);
            }
            if (type == null) {
                return getNumericOrStringRollupDataForRange(locator, range, gran, rollupType);
            }

            DataType metricType = new DataType((String) type);
            if (!DataType.isKnownMetricType(metricType)) {
                return getNumericOrStringRollupDataForRange(locator, range, gran, rollupType);
            }
            if (metricType.equals(DataType.STRING)) {
                gran = Granularity.FULL;
                return getStringMetricDataForRange(locator, range, gran);
            } else if (metricType.equals(DataType.BOOLEAN)) {
                gran = Granularity.FULL;
                return getBooleanMetricDataForRange(locator, range, gran);
            } else {
                return getNumericMetricDataForRange(locator, range, gran, rollupType, metricType);
            }

        } catch (CacheException e) {
            log.warn("Caught exception trying to find metric type from meta cache for locator " + locator.toString(), e);
            return getNumericOrStringRollupDataForRange(locator, range, gran, RollupType.BF_BASIC);
        }
    }

    // TODO: This should be the only method all output handlers call. We should be able to deprecate
    // other individual metric fetch methods once this gets in.
    public Map<Locator, MetricData> getDatapointsForRange(List<Locator> locators, Range range, Granularity gran) {
        ListMultimap<ColumnFamily, Locator> locatorsByCF =
                ArrayListMultimap.create();
        Map<Locator, MetricData> results = new HashMap<Locator, MetricData>();
        Map<Locator, MetricData> enumResults;

        List<Locator> enLocators = new ArrayList<Locator>();
        for (Locator locator : locators) {
            try {
                RollupType rollupType = RollupType.fromString((String)
                        metaCache.get(locator, MetricMetadata.ROLLUP_TYPE.name().toLowerCase()));
                DataType dataType = getDataType(locator, MetricMetadata.TYPE.name().toLowerCase());
                if (rollupType == RollupType.ENUM) {
                    enLocators.add(locator);
                }
                else {
                    ColumnFamily cf = CassandraModel.getColumnFamily(rollupType, dataType, gran);
                    List<Locator> locs = locatorsByCF.get(cf);
                    locs.add(locator);
                }
            } catch (Exception e) {
                // pass for now. need metric to figure this stuff out.
                log.error(String.format("error getting datapoints for locator %s, range %s, granularity %s", locator, range.toString(), gran.toString()), e);
            }
        }

        for (ColumnFamily CF : locatorsByCF.keySet()) {
            List<Locator> locs = locatorsByCF.get(CF);
            results.putAll(getNumericDataForRangeLocatorList(range, gran, CF, locs));
        }

        enumResults = getEnumMetricDataForRangeForLocatorList(enLocators, range, gran);
        results.putAll(enumResults);

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

        return new MetricData(points, metaCache.getUnitString(locator), MetricData.Type.STRING);
    }

    protected MetricData getEnumMetricDataForRange(final Locator locator, final Range range, Granularity gran) {
        Map<Locator, MetricData> metricDataMap = getEnumMetricDataForRangeForLocatorList(new ArrayList<Locator>(){{add (locator); }}, range, gran);
        return metricDataMap.get(locator);
    }

    public  Map<Locator, MetricData> getEnumMetricDataForRangeForLocatorList(final List<Locator> locator, final Range range, final Granularity gran) {
        final ColumnFamily<Locator, Long> CF = CassandraModel.getColumnFamily(RollupType.classOf(RollupType.ENUM, gran), gran);

        Future<Map<Locator, ColumnList<Long>>> enumValuesFuture = taskExecutor.submit(new Callable() {
            @Override
            public Map<Locator, ColumnList<Long>> call() throws Exception {
                return getEnumHashMappings(new ArrayList<Locator>(locator));
            }

        });

        Future<Map<Locator, MetricData>> pointsFuture = taskExecutor.submit(new Callable() {
            @Override
            public Map<Locator, MetricData> call() throws Exception {
                return getNumericDataForRangeLocatorList(range, gran, CF, locator);
            }
        });

        Map<Locator, MetricData> metricDataMap;
        Map<Locator, MetricData> resultMap = new HashMap<Locator, MetricData>();
        try {
            Map<Locator, ColumnList<Long>> enumValues =  enumValuesFuture.get();
            metricDataMap = pointsFuture.get();
            for (Locator l : metricDataMap.keySet()) {
                Points p = transformEnumValueHashesToStrings(metricDataMap.get(l), enumValues.get(l));
                MetricData m = new MetricData(p, metaCache.getUnitString(l), MetricData.Type.ENUM);
                resultMap.put(l,m);
            }
        } catch (InterruptedException e) {
            log.error("Interrupted Exception during query of  Enum metrics",e);
        } catch (ExecutionException e) {
            log.error("Execution Exception during query of Enum metrics", e);
        }

        return resultMap;
    }

    private MetricData getBooleanMetricDataForRange(Locator locator, Range range, Granularity gran) {
        Points<Boolean> points = new Points<Boolean>();
        ColumnList<Long> results = getColumnsFromDB(locator, CassandraModel.CF_METRICS_STRING, range);

        for (Column<Long> column : results) {
            try {
                points.add(new Points.Point<Boolean>(column.getName(), Boolean.valueOf( column.getValue(StringSerializer.get() ))));
            } catch (RuntimeException ex) {
                log.error("Problem deserializing Boolean data for " + locator + " (" + range + ") from " +
                        CassandraModel.CF_METRICS_STRING.getName(), ex);
            }
        }

        return new MetricData(points, metaCache.getUnitString(locator), MetricData.Type.BOOLEAN);
    }

    // todo: replace this with methods that pertain to type (which can be used to derive a serializer).
    private MetricData getNumericMetricDataForRange(Locator locator, Range range, Granularity gran, RollupType rollupType, DataType dataType) {
        ColumnFamily<Locator, Long> CF = CassandraModel.getColumnFamily(rollupType, dataType, gran);
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

        return new MetricData(points, metaCache.getUnitString(locator), MetricData.Type.NUMBER);
    }

    // gets called when we DO NOT know what the data type is (numeric, string, etc.)
    private MetricData getNumericOrStringRollupDataForRange(Locator locator, Range range, Granularity gran, RollupType rollupType) {
        Instrumentation.markScanAllColumnFamilies();

        final MetricData metricData = getNumericMetricDataForRange(locator, range, gran, rollupType, DataType.NUMERIC);

        if (metricData.getData().getPoints().size() > 0) {
            return metricData;
        }

        return getStringMetricDataForRange(locator, range, gran);
    }

    private MetricData transformColumnsToMetricData(Locator locator, ColumnList<Long> columns,
                                                    Granularity gran) {
        try {
            RollupType rollupType = RollupType.fromString(metaCache.get(locator, rollupTypeCacheKey));
            DataType dataType = getDataType(locator, dataTypeCacheKey);
            String unit = metaCache.getUnitString(locator);
            MetricData.Type outputType = MetricData.Type.from(rollupType, dataType);
            Points points = getPointsFromColumns(columns, rollupType, dataType, gran);
            MetricData data = new MetricData(points, unit, outputType);
            return data;
        } catch (Exception e) {
            return null;
        }
    }

    private DataType getDataType(Locator locator, String dataTypeCacheKey) throws CacheException{
        String meta = metaCache.get(locator, dataTypeCacheKey);
        if (meta != null) {
            return new DataType(meta);
        }
        return DataType.NUMERIC;
    }

    private Points getPointsFromColumns(ColumnList<Long> columnList, RollupType rollupType,
                                        DataType dataType, Granularity gran) {
        Points points = new Points();

        AbstractSerializer serializer = serializerFor(rollupType, dataType, gran);
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

    private <T extends Rollup> Points<T> transformEnumValueHashesToStrings(MetricData metricData, ColumnList<Long> enumvalues) {
        Map<Long, String> hash2enumValues = getEnumValueFromHashes(enumvalues);
        Points<T> pointsEnum = new Points<T>();

        Map<Long, Points.Point<T>> pointsMap = metricData.getData().getPoints();

        for (Long timestamp : pointsMap.keySet()) {
            BluefloodEnumRollup enumRollup = (BluefloodEnumRollup)pointsMap.get(timestamp).getData();
            for (Long hash : enumRollup.getHashedEnumValuesWithCounts().keySet()) {
                String enumValueString = hash2enumValues.get(hash);
                enumRollup.getStringEnumValuesWithCounts().put(enumValueString, enumRollup.getHashedEnumValuesWithCounts().get(hash));
                pointsEnum.add(new Points.Point<T>(timestamp,(T)enumRollup));
            }
        }
        return pointsEnum;
    }

    private Map<Long, String> getEnumValueFromHashes(ColumnList<Long> enumValues) {
        HashMap<Long,String> hash2enumValues = new HashMap<Long, String>();

        for (Column<Long> col: enumValues) {
            hash2enumValues.put(col.getName(), col.getStringValue());
        }

        return hash2enumValues;
    }


    /**
     * This method locates all values of enums from metrics_enum column family by their {@link com.rackspacecloud.blueflood.types.Locator}.
     * The result is organized in a map of Locator -> ColumnList. The ColumnList is a list
     * of columns, each column is a pair of name and value. The name will be the hash
     * value of an enum, and the value would be the string value of the enum.
     *
     * @param locators
     * @return
     */
    private Map<Locator, ColumnList<Long>> getEnumHashMappings(final List<Locator> locators) {

        final Map<Locator, ColumnList<Long>> columns = new HashMap<Locator, ColumnList<Long>>();

        try {
            OperationResult<Rows<Locator, Long>> query = getKeyspace()
                    .prepareQuery(CassandraModel.CF_METRICS_ENUM)
                    .getKeySlice(locators)
                    .execute();

            for (Row<Locator, Long> row : query.getResult()) {
                columns.put(row.getKey(), row.getColumns());
            }
        } catch (ConnectionException e) {
            if (e instanceof NotFoundException) { // TODO: Not really sure what happens when one of the keys is not found.
                Instrumentation.markNotFound(CassandraModel.CF_METRICS_ENUM_NAME);
            } else {
                log.warn("Enum read query failed for column family " + CassandraModel.CF_METRICS_ENUM_NAME, e);
                Instrumentation.markReadError(e);
            }
        }
        return columns;
    }

    /**
     * This method locates all values of enums from metrics_enum column family by their {@link com.rackspacecloud.blueflood.types.Locator}.
     * The result is organized in a map of Locator -> list of String. The string
     * is the string enum values.
     *
     * @param locators
     * @return
     */
    public Map<Locator, List<String>> getEnumStringMappings(final List<Locator> locators) {
        // TODO: add this to our read/write metrics
        final Map<Locator, List<String>> map = new HashMap<Locator, List<String>>();

        try {
            OperationResult<Rows<Locator, Long>> query = getKeyspace()
                    .prepareQuery(CassandraModel.CF_METRICS_ENUM)
                    .getKeySlice(locators)
                    .execute();

            for (Row<Locator, Long> row : query.getResult()) {
                ColumnList<Long> cols = row.getColumns();
                List<String> enumStrings = new ArrayList<String>();
                for (Column col : cols) {
                    enumStrings.add(col.getStringValue());
                }
                map.put(row.getKey(), enumStrings);
            }
        } catch (PoolTimeoutException ex) {
            Instrumentation.markPoolExhausted();
            Instrumentation.markReadError();
        } catch( NotFoundException ex) {
            // TODO: Not really sure what happens when one of the keys is not found.
            Instrumentation.markNotFound(CassandraModel.CF_METRICS_ENUM_NAME);
        } catch (ConnectionException e) {
            log.warn("Enum String read query failed for column family " + CassandraModel.CF_METRICS_ENUM_NAME, e);
            Instrumentation.markReadError();
        }
        return map;
    }
}
