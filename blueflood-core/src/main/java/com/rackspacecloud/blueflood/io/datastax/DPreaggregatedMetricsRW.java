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
import com.rackspacecloud.blueflood.cache.LocatorCache;
import com.rackspacecloud.blueflood.exceptions.InvalidDataException;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.*;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * This class deals with reading/writing metrics to the metrics_preaggregated_* column families
 * using Datastax driver
 */
public class DPreaggregatedMetricsRW extends DAbstractMetricsRW implements PreaggregatedRW {

    private static final Logger LOG = LoggerFactory.getLogger(DPreaggregatedMetricsRW.class);
    private static final Timer waitResultsTimer = Metrics.timer(DPreaggregatedMetricsRW.class, "PreAggr Metrics Wait Write Results");

    private final DCounterIO counterIO = new DCounterIO();
    private final DGagueIO gaugeIO = new DGagueIO();
    private final DSetIO setIO = new DSetIO();
    private final DTimerIO timerIO = new DTimerIO();

    // a map of RollupType to its IO class that knows
    // how to read/write that particular type of rollup
    private final Map<RollupType, DAbstractMetricIO> rollupTypeToIO =
            new HashMap<RollupType, DAbstractMetricIO>();

    /**
     * Constructor
     * @param locatorIO
     * @param isRecordingDelayedMetrics
     */
    public DPreaggregatedMetricsRW(DAbstractMetricIO enumIO, LocatorIO locatorIO, DelayedLocatorIO delayedLocatorIO,
                                   boolean isRecordingDelayedMetrics, Clock clock) {
        super(locatorIO, delayedLocatorIO, isRecordingDelayedMetrics, clock);
        rollupTypeToIO.put(RollupType.COUNTER, counterIO);
        rollupTypeToIO.put(RollupType.ENUM, enumIO);
        rollupTypeToIO.put(RollupType.GAUGE, gaugeIO);
        rollupTypeToIO.put(RollupType.SET, setIO);
        rollupTypeToIO.put(RollupType.TIMER, timerIO);
    }

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
                    DAbstractMetricIO io = rollupTypeToIO.get(rollupType);
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

                    if ( !LocatorCache.getInstance().isLocatorCurrent(locator) ) {
                        locatorIO.insertLocator(locator);
                        LocatorCache.getInstance().setLocatorCurrent(locator);
                    }  else {
                        LOG.trace("insertMetrics(): not inserting locator " + locator);
                    }

                    if (isRecordingDelayedMetrics) {
                        insertLocatorIfDelayed(metric);
                    }

                }
            }

            for (ResultSetFuture future : futureLocatorMap.keySet()) {
                try {
                    future.getUninterruptibly().all();
                    if (granularity == Granularity.FULL) {
                        Instrumentation.markFullResPreaggregatedMetricWritten();
                    }
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
                                                          Granularity granularity) /*throws IOException*/ {

        String columnFamily = CassandraModel.getPreaggregatedColumnFamilyName(granularity);

        return getDatapointsForRange( locators, range, columnFamily, granularity );
    }

    /**
     * Return the appropriate IO object which interacts with the Cassandra database.
     *
     * For Preaggregated Metrics, only rollup type is required.
     * For Basic Numeric Metrics, the granularity is required as metrics_full is handled differently
     * than the other granularities.
     *
     * @param rollupType
     * @param granularity
     * @return
     */
    @Override
    public DAbstractMetricIO getIO( String rollupType, Granularity granularity ) {

        // find out the rollupType for this locator
        RollupType rType = RollupType.fromString( rollupType );

        // get the right PreaggregatedIO class that can process
        // this rollupType
        DAbstractMetricIO io = rollupTypeToIO.get( rType );

        if (io == null) {
            throw new InvalidDataException( String.format("getIO: unsupported rollupType=%s", rollupType));
        }

        return io;
    }
}

