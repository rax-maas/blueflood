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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.rackspacecloud.blueflood.exceptions.CacheException;
import com.rackspacecloud.blueflood.exceptions.InvalidDataException;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxWriter;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.SingleRollupWriteContext;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * This class deals with reading/writing metrics to the metrics_preaggregated_* column families
 * using Datastax driver
 */
public class DPreaggregatedMetricsRW extends AbstractMetricsRW {

    private static final Logger LOG = LoggerFactory.getLogger(DPreaggregatedMetricsRW.class);

    private final DCounterIO counterIO = new DCounterIO();
    private final DEnumIO enumIO = new DEnumIO();
    private final DGagueIO gaugeIO = new DGagueIO();
    private final DSetIO setIO = new DSetIO();
    private final DTimerIO timerIO = new DTimerIO();
    private final LocatorIO locatorIO = IOContainer.fromConfig().getLocatorIO();

    // a map of RollupType to its IO class that knows
    // how to read/write that particular type of rollup
    private final Map<RollupType, DAbstractPreaggregatedIO> rollupTypeToIO =
            new HashMap<RollupType, DAbstractPreaggregatedIO>() {{
                    put(RollupType.COUNTER, counterIO);
                    put(RollupType.ENUM, enumIO);
                    put(RollupType.GAUGE, gaugeIO);
                    put(RollupType.SET, setIO);
                    put(RollupType.TIMER, timerIO);
    }};

    /**
     * Inserts a collection of metrics to the metrics_preaggregated_full column family
     *
     * @param metrics
     * @throws IOException
     */
    @Override
    public void insertMetrics(Collection<IMetric> metrics) throws IOException {
        insertMetrics(metrics, Granularity.FULL);
    }

    /**
     * Inserts a collection of rolled up metrics to the metrics_preaggregated_{granularity} column family.
     * Only our tests should call this method. Services should call either insertMetrics(Collection metrics)
     * or insertRollups()
     *
     * @param metrics
     * @throws IOException
     */
    @VisibleForTesting
    @Override
    public void insertMetrics(Collection<IMetric> metrics,
                                 Granularity granularity) throws IOException {
        Timer.Context ctx = Instrumentation.getWriteTimerContext(
                                CassandraModel.getPreaggregatedColumnFamilyName(granularity));
        try {
            Map<ResultSetFuture, Locator> futureLocatorMap = new HashMap<ResultSetFuture, Locator>();
            Multimap<Locator, IMetric> map = asMultimap(metrics);
            for (Locator locator : map.keySet()) {
                for (IMetric metric : map.get(locator)) {
                    RollupType rollupType = metric.getRollupType();

                    // lookup the right io object
                    DAbstractPreaggregatedIO io = rollupTypeToIO.get(rollupType);
                    if ( io == null ) {
                        throw new InvalidDataException(
                                String.format("insertMetrics(locator=%s, granularity=%s): unsupported preaggregated rollupType=%s",
                                        locator, granularity, rollupType.name()));
                    }

                    if (!(metric.getMetricValue() instanceof Rollup)) {
                        throw new InvalidDataException(
                                String.format("insertMetrics(locator=%s, granularity=%s): metric value %s is not type Rollup",
                                        locator, granularity, metric.getMetricValue().getClass().getSimpleName())
                        );
                    }
                    ResultSetFuture future = io.putAsync(locator, metric.getCollectionTime(),
                                                        (Rollup) metric.getMetricValue(),
                                                        granularity, metric.getTtlInSeconds());
                    futureLocatorMap.put(future, locator);

                    if ( !isLocatorCurrent(locator) ) {
                        locatorIO.insertLocator(locator);
                        setLocatorCurrent(locator);
                    }  else {
                        LOG.debug("insertMetrics(): not inserting locator " + locator);
                    }
                }
            }

            for (ResultSetFuture future : futureLocatorMap.keySet()) {
                try {
                    future.getUninterruptibly().all();
                } catch (Exception ex) {
                    Instrumentation.markWriteError();
                    LOG.error(String.format("error writing preaggregated metric for locator %s, granularity %s",
                            futureLocatorMap.get(future), granularity), ex);
                }
            }
        } finally {
            ctx.stop();
        }
    }

    /**
     * This method inserts a collection of metrics in the
     * {@link com.rackspacecloud.blueflood.service.SingleRollupWriteContext}
     * objects to the appropriate Cassandra column family
     *
     * @param writeContexts
     * @throws IOException
     */
    @Override
    public void insertRollups(List<SingleRollupWriteContext> writeContexts) throws IOException {

        if (writeContexts.size() == 0) {
            return;
        }

        Map<ResultSetFuture, SingleRollupWriteContext> futureLocatorMap = new HashMap<ResultSetFuture, SingleRollupWriteContext>();
        Timer.Context ctx = Instrumentation.getWriteTimerContext(writeContexts.get(0).getDestinationCF().getName());
        try {
            for (SingleRollupWriteContext writeContext : writeContexts) {
                Rollup rollup = writeContext.getRollup();
                Locator locator = writeContext.getLocator();
                Granularity granularity = writeContext.getGranularity();
                int ttl = getTtl(locator, rollup.getRollupType(), granularity);

                // lookup the right writer
                RollupType rollupType = writeContext.getRollup().getRollupType();
                DAbstractPreaggregatedIO io = rollupTypeToIO.get(rollupType);
                if ( io == null ) {
                    throw new InvalidDataException(
                            String.format("insertRollups(locator=%s, granularity=%s): unsupported preaggregated rollupType=%s",
                                    locator, granularity, rollupType.name()));
                }

                ResultSetFuture future = io.putAsync(locator, writeContext.getTimestamp(), rollup, writeContext.getGranularity(), ttl);
                futureLocatorMap.put(future, writeContext);
            }

            for (ResultSetFuture future : futureLocatorMap.keySet()) {
                try {
                    future.getUninterruptibly().all();
                } catch (Exception ex) {
                    SingleRollupWriteContext writeContext = futureLocatorMap.get(future);
                    LOG.error(String.format("error writing to locator %s, granularity %s", writeContext.getLocator(), writeContext.getGranularity()), ex);
                }
            }
        } finally {
            ctx.stop();
        }
    }

    /**
     * Fetches a {@link com.rackspacecloud.blueflood.types.Points} object for a
     * particular locator and rollupType from the specified column family and
     * range
     *
     * @param locator
     * @param rollupType
     * @param range
     * @param columnFamilyName
     * @param <T> the type of Rollup object
     * @return
     */
    @Override
    public <T extends Rollup> Points<T> getDataToRollup(final Locator locator,
                                                        RollupType rollupType,
                                                        Range range,
                                                        String columnFamilyName) throws IOException {
        Timer.Context ctx = Instrumentation.getReadTimerContext(columnFamilyName);
        try {
            // read the rollup object from the proper IO class
            DAbstractPreaggregatedIO io = rollupTypeToIO.get(rollupType);
            Table<Locator, Long, Rollup> locatorTimestampRollup = io.getRollupsForLocator(locator, columnFamilyName, range);

            // transform them to Points
            Points<T> points = new Points<T>();
            for (Table.Cell<Locator, Long, Rollup> cell : locatorTimestampRollup.cellSet()) {
                points.add(new Points.Point<T>(cell.getColumnKey(), (T) cell.getValue()));
            }
            return points;
        } finally {
            ctx.stop();
        }
    }

    /**
     * Fetches {@link com.rackspacecloud.blueflood.outputs.formats.MetricData} objects for the
     * specified {@link com.rackspacecloud.blueflood.types.Locator} and
     * {@link com.rackspacecloud.blueflood.types.Range} from the specified column family
     *
     * @param locator
     * @param range
     * @param granularity
     * @return
     */
    @Override
    public MetricData getDatapointsForRange(final Locator locator,
                                            Range range,
                                            Granularity granularity) throws IOException {
        Map<Locator, MetricData> result = getDatapointsForRange(new ArrayList<Locator>() {{ add(locator); }}, range, granularity);
        return result.get(locator);
    }

    /**
     * Fetches {@link com.rackspacecloud.blueflood.outputs.formats.MetricData} objects for the
     * specified list of {@link com.rackspacecloud.blueflood.types.Locator} and
     * {@link com.rackspacecloud.blueflood.types.Range} from the specified column family
     *
     * @param locators
     * @param range
     * @param granularity
     * @return
     */
    @Override
    public Map<Locator, MetricData> getDatapointsForRange(List<Locator> locators,
                                                          Range range,
                                                          Granularity granularity) throws IOException {

        Timer.Context ctx = Instrumentation.getReadTimerContext(
                                                CassandraModel.getPreaggregatedColumnFamilyName(granularity));

        try {
            // in this loop, we will fire all the executeAsync() of
            // various select statements, the collect all of the
            // ResultSetFutures
            Map<Locator, List<ResultSetFuture>> locatorToFuturesMap = new HashMap<Locator, List<ResultSetFuture>>();
            Map<Locator, RollupType> locatorRollupTypeMap = new HashMap<Locator, RollupType>();
            for (Locator locator : locators) {
                try {
                    // find out the rollupType for this locator
                    RollupType rollupType = RollupType.fromString(
                            metadataCache.get(locator, MetricMetadata.ROLLUP_TYPE.name().toLowerCase()));

                    // get the right PreaggregatedIO class that can process
                    // this rollupType
                    DAbstractPreaggregatedIO preaggregatedIO = rollupTypeToIO.get(rollupType);
                    if (preaggregatedIO == null) {
                        throw new IllegalArgumentException(String.format("dont know how to handle locator=%s with rollupType=%s", locator, rollupType.toString()));
                    }

                    // put everything in a map of locator -> rollupType, so
                    // we can use em up later
                    locatorRollupTypeMap.put(locator, rollupType);

                    // do the query
                    List<ResultSetFuture> selectFutures = preaggregatedIO.selectForLocatorRangeGranularity(locator, range, granularity);

                    // add all ResultSetFutures for a particular locator together
                    List<ResultSetFuture> existing = locatorToFuturesMap.get(locator);
                    if (existing == null) {
                        existing = new ArrayList<ResultSetFuture>();
                        locatorToFuturesMap.put(locator, existing);
                    }
                    existing.addAll(selectFutures);

                } catch (CacheException ex) {
                    LOG.error(String.format("Error looking up locator %s in cache", locator), ex);
                }
            }
            return resultSetsToMetricData(locatorToFuturesMap, locatorRollupTypeMap, granularity);
        } finally {
            ctx.stop();
        }
    }

    /**
     * Converts a list of {@link com.datastax.driver.core.ResultSetFuture} for each
     * {@link com.rackspacecloud.blueflood.types.Locator} to
     * {@link com.rackspacecloud.blueflood.outputs.formats.MetricData} object.
     *
     * @param resultSets
     * @param locatorRollupType
     * @param granularity
     * @return
     */
    protected Map<Locator, MetricData> resultSetsToMetricData(Map<Locator,
                                                              List<ResultSetFuture>> resultSets,
                                                              Map<Locator, RollupType> locatorRollupType,
                                                              Granularity granularity) {

        // iterate through all ResultSetFuture
        Map<Locator, MetricData> locatorMetricDataMap = new HashMap<Locator, MetricData>();
        for (Map.Entry<Locator, List<ResultSetFuture>> entry : resultSets.entrySet() ) {
            Locator locator = entry.getKey();
            List<ResultSetFuture> futures = entry.getValue();

            try {
                RollupType rollupType = locatorRollupType.get(locator);

                DAbstractPreaggregatedIO io = rollupTypeToIO.get(rollupType);

                // get ResultSets to a Table of locator, timestamp, rollup
                Table<Locator, Long, Rollup> locatorTimestampRollup = io.toLocatorTimestampRollup(futures, locator, granularity);

                // convert to Points and MetricData
                Points<Rollup> points = convertToPoints(locatorTimestampRollup.row(locator));

                // get the dataType for this locator
                DataType dataType = getDataType(locator, MetricMetadata.TYPE.name().toLowerCase());

                // create MetricData
                MetricData.Type outputType = MetricData.Type.from(rollupType, dataType);
                MetricData metricData = new MetricData(points, getUnitString(locator), outputType);
                locatorMetricDataMap.put(locator, metricData);

            } catch (CacheException ex) {
                Instrumentation.markReadError();
                LOG.error(String.format("error getting dataType for locator %s, granularity %s",
                        locator, granularity), ex);
            }
        }
        return locatorMetricDataMap;
    }
}
