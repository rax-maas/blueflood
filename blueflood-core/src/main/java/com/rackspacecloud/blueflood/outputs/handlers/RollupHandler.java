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

package com.rackspacecloud.blueflood.outputs.handlers;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.io.SearchResult;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class RollupHandler {
    private static final Logger log = LoggerFactory.getLogger(RollupHandler.class);

    protected final Meter rollupsByPointsMeter = Metrics.meter(RollupHandler.class, "BF-API", "Get rollups by points");
    protected final Meter rollupsByGranularityMeter = Metrics.meter(RollupHandler.class, "BF-API", "Get rollups by gran");
    protected final Meter rollupsRepairEntireRange = Metrics.meter(RollupHandler.class, "BF-API", "Rollups repaired - entire range");
    protected final Meter rollupsRepairedLeft = Metrics.meter(RollupHandler.class, "BF-API", "Rollups repaired - left");
    protected final Meter rollupsRepairedRight = Metrics.meter(RollupHandler.class, "BF-API", "Rollups repaired - right");
    protected final Meter rollupsRepairEntireRangeEmpty = Metrics.meter(RollupHandler.class, "BF-API", "Rollups repaired - entire range - no data");
    protected final Meter rollupsRepairedLeftEmpty = Metrics.meter(RollupHandler.class, "BF-API", "Rollups repaired - left - no data");
    protected final Meter rollupsRepairedRightEmpty = Metrics.meter(RollupHandler.class, "BF-API", "Rollups repaired - right - no data");
    protected static final Timer metricsFetchTimer = Metrics.timer(RollupHandler.class, "Get metrics from db");
    protected static final Timer metricsFetchTimerMPlot = Metrics.timer(RollupHandler.class, "Get metrics from db - mplot");
    protected static final Timer rollupsCalcOnReadTimer = Metrics.timer(RollupHandler.class, "Rollups calculation on read");
    protected static final Timer rollupsCalcOnReadTimerMPlot = Metrics.timer(RollupHandler.class, "Rollups calculation on read - mplot");
    protected final Histogram numFullPointsReturned = Metrics.histogram(RollupHandler.class, "Full res points returned");
    protected final Histogram numRollupPointsReturned = Metrics.histogram(RollupHandler.class, "Rollup points returned");
    protected final Histogram numHistogramPointsReturned = Metrics.histogram(RollupHandler.class, "Histogram points returned");
    private static final Meter exceededQueryTimeout = Metrics.meter(RollupHandler.class, "Batched Metrics Query Duration Exceeded Timeout");
    private static final Histogram queriesSizeHist = Metrics.histogram(RollupHandler.class, "Total queries");

    private static final boolean ROLLUP_REPAIR = Configuration.getInstance().getBooleanProperty(CoreConfig.REPAIR_ROLLUPS_ON_READ);
    private ExecutorService ESUnitExecutor = null;
    private ListeningExecutorService rollupsOnReadExecutor = null;
    /*
      Timeout for rollups on read applicable only when operations are done async. for sync rollups on read
      it will be the astyanax operation timeout.
      TODO: Hard-coded for now, make configurable if need be
     */
    private TimeValue rollupOnReadTimeout = new TimeValue(10, TimeUnit.SECONDS);

    public RollupHandler() {
        if (Util.shouldUseESForUnits()) {
            // The number of threads getting used for ES_UNIT_THREADS, should at least be equal netty worker threads
            int ESthreadCount = Configuration.getInstance().getIntegerProperty(CoreConfig.ES_UNIT_THREADS);
            ESUnitExecutor = new ThreadPoolBuilder().withUnboundedQueue()
                    .withCorePoolSize(ESthreadCount)
                    .withMaxPoolSize(ESthreadCount).withName("Rolluphandler ES executors").build();
        }
        if (!Configuration.getInstance().getBooleanProperty(CoreConfig.TURN_OFF_RR_MPLOT)) {
            ThreadPoolExecutor rollupsOnReadExecutors = new ThreadPoolBuilder().withUnboundedQueue()
                    .withCorePoolSize(Configuration.getInstance().getIntegerProperty(CoreConfig.ROLLUP_ON_READ_THREADS))
                    .withMaxPoolSize(Configuration.getInstance().getIntegerProperty(CoreConfig.ROLLUP_ON_READ_THREADS))
                    .withName("Rollups on Read Executors").build();
            rollupsOnReadExecutor = MoreExecutors.listeningDecorator(rollupsOnReadExecutors);
        }
    }

    private enum plotTimers {
        SPLOT_TIMER(metricsFetchTimer),
        MPLOT_TIMER(metricsFetchTimerMPlot);
        private Timer timer;

        private plotTimers(Timer timer) {
            this.timer = timer;
        }
    }

    private enum rollupsOnReadTimers {
        RR_SPLOT_TIMER(rollupsCalcOnReadTimer),
        RR_MPLOT_TIMER(rollupsCalcOnReadTimerMPlot);
        private Timer timer;

        private rollupsOnReadTimers (Timer timer) {
            this.timer = timer;
        }
    }

    public Map<Locator, MetricData> getRollupByGranularity(
            final String tenantId,
            final List<String> metrics,
            final long from,
            final long to,
            final Granularity g) {

        final Timer.Context ctx = metrics.size() == 1 ? plotTimers.SPLOT_TIMER.timer.time() : plotTimers.MPLOT_TIMER.timer.time();
        Future<List<SearchResult>> unitsFuture = null;
        List<SearchResult> units = null;
        List<Locator> locators = new ArrayList<Locator>();

        for (String metric : metrics) {
            locators.add(Locator.createLocatorFromPathComponents(tenantId, metric));
        }

        queriesSizeHist.update(locators.size());

        if (Util.shouldUseESForUnits()) {
             unitsFuture = ESUnitExecutor.submit(new Callable() {

                 @Override
                 public List<SearchResult> call() throws Exception {
                     DiscoveryIO discoveryIO = (DiscoveryIO) ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.DISCOVERY_MODULES);

                     if (discoveryIO == null) {
                         log.warn("USE_ES_FOR_UNITS has been set to true, but no discovery module found." +
                                 " Please check your config");
                         return null;
                     }
                     return discoveryIO.search(tenantId, metrics);
                 }
             });
        }
        final Map<Locator,MetricData> metricDataMap = AstyanaxReader.getInstance().getDatapointsForRange(
                locators,
                new Range(g.snapMillis(from), to),
                g);

        if (unitsFuture != null) {
            try {
                units = unitsFuture.get();
                for (SearchResult searchResult : units) {
                    Locator locator = Locator.createLocatorFromPathComponents(searchResult.getTenantId(), searchResult.getMetricName());
                    if (metricDataMap.containsKey(locator))
                        metricDataMap.get(locator).setUnit(searchResult.getUnit());
                }
            } catch (Exception e) {
                log.warn("Exception encountered while getting units from ES, unit will be set to unknown in query results");
                log.debug(e.getMessage(), e);
            }
        }

        if (locators.size() == 1) {
            for (final Map.Entry<Locator, MetricData> metricData : metricDataMap.entrySet()) {
                Timer.Context context = rollupsOnReadTimers.RR_SPLOT_TIMER.timer.time();
                repairMetrics(metricData.getKey(), metricData.getValue(), from, to, g);
                context.stop();
            }
        } else if (locators.size() > 1 && Configuration.getInstance().getBooleanProperty(CoreConfig.TURN_OFF_RR_MPLOT) == false) {
            Timer.Context context = rollupsOnReadTimers.RR_MPLOT_TIMER.timer.time();
            ArrayList<ListenableFuture<Boolean>> futures = new ArrayList<ListenableFuture<Boolean>>();
            for (final Map.Entry<Locator, MetricData> metricData : metricDataMap.entrySet()) {
                futures.add(
                        rollupsOnReadExecutor.submit(new Callable<Boolean>() {
                            @Override
                            public Boolean call() {
                                return repairMetrics(metricData.getKey(), metricData.getValue(), from, to, g);
                            }
                        }));
            }
            ListenableFuture<List<Boolean>> aggregateFuture = Futures.allAsList(futures);
            try {
                aggregateFuture.get(rollupOnReadTimeout.getValue(), rollupOnReadTimeout.getUnit());
            } catch (Exception e) {
                aggregateFuture.cancel(true);
                exceededQueryTimeout.mark();
                log.warn(String.format("Exception encountered while doing rollups on read, incomplete rollups will be returned. %s", e.getMessage()));
            }
            context.stop();
        }
        ctx.stop();
        return metricDataMap;
    }

    private Boolean repairMetrics (Locator locator, MetricData metricData, final long from,
                                   final long to,
                                   final Granularity g) {
        boolean isRollable = metricData.getType().equals(MetricData.Type.NUMBER.toString())
                || metricData.getType().equals(MetricData.Type.HISTOGRAM.toString());
        Boolean retValue = false;

        // if Granularity is FULL, we are missing raw data - can't generate that
        if (ROLLUP_REPAIR && isRollable && g != Granularity.FULL && metricData != null) {
            if (metricData.getData().isEmpty()) { // data completely missing for range. complete repair.
                rollupsRepairEntireRange.mark();
                List<Points.Point> repairedPoints = repairRollupsOnRead(locator, g, from, to);
                for (Points.Point repairedPoint : repairedPoints) {
                    metricData.getData().add(repairedPoint);
                }

                if (repairedPoints.isEmpty()) {
                    rollupsRepairEntireRangeEmpty.mark();
                }
            } else {
                long actualStart = minTime(metricData.getData());
                long actualEnd = maxTime(metricData.getData());

                // If the returned start is greater than 'from', we are missing a portion of data.
                if (actualStart > from) {
                    rollupsRepairedLeft.mark();
                    List<Points.Point> repairedLeft = repairRollupsOnRead(locator, g, from, actualStart);
                    for (Points.Point repairedPoint : repairedLeft) {
                        metricData.getData().add(repairedPoint);
                    }

                    if (repairedLeft.isEmpty()) {
                        rollupsRepairedLeftEmpty.mark();
                    }
                }

                // If the returned end timestamp is less than 'to', we are missing a portion of data.
                if (actualEnd + g.milliseconds() <= to) {
                    rollupsRepairedRight.mark();
                    List<Points.Point> repairedRight = repairRollupsOnRead(locator, g, actualEnd + g.milliseconds(), to);
                    for (Points.Point repairedPoint : repairedRight) {
                        metricData.getData().add(repairedPoint);
                    }

                    if (repairedRight.isEmpty()) {
                        rollupsRepairedRightEmpty.mark();
                    }
                }
            }
            retValue = true;
        }

        if (g == Granularity.FULL) {
            numFullPointsReturned.update(metricData.getData().getPoints().size());
        } else {
            numRollupPointsReturned.update(metricData.getData().getPoints().size());
        }
        return retValue;
    }

    private List<Points.Point> repairRollupsOnRead(Locator locator, Granularity g, long from, long to) {
        List<Points.Point> repairedPoints = new ArrayList<Points.Point>();

        Iterable<Range> ranges = Range.rangesForInterval(g, g.snapMillis(from), to);
        for (Range r : ranges) {
            try {
                MetricData data = AstyanaxReader.getInstance().getDatapointsForRange(locator, r, Granularity.FULL);
                Points dataToRoll = data.getData();
                if (dataToRoll.isEmpty()) {
                    continue;
                }
                Rollup rollup = RollupHandler.rollupFromPoints(dataToRoll);

                if (rollup.hasData()) {
                    repairedPoints.add(new Points.Point(r.getStart(), rollup));
                }
            } catch (IOException ex) {
                log.error("Exception computing rollups during read: ", ex);
            }
        }

        return repairedPoints;
    }

    private static long minTime(Points<?> points) {
        long min = Long.MAX_VALUE;
        for (long time : points.getPoints().keySet())
            min = Math.min(min, time);
        return min;
    }

    private static long maxTime(Points<?> points) {
        long max = Long.MIN_VALUE;
        for (long time : points.getPoints().keySet())
            max = Math.max(max, time);
        return max;
    }

    // note: similar thing happening in RollupRunnable.getRollupComputer(), but we don't have access to RollupType here.
    private static Rollup rollupFromPoints(Points points) throws IOException {
        Class rollupTypeClass = points.getDataClass();
        if (rollupTypeClass.equals(SimpleNumber.class)) {
            return Rollup.BasicFromRaw.compute(points);
        } else if (rollupTypeClass.equals(BluefloodCounterRollup.class)) {
            return Rollup.CounterFromCounter.compute(points);
        } else if (rollupTypeClass.equals(BluefloodSetRollup.class)) {
            return Rollup.SetFromSet.compute(points);
        } else if (rollupTypeClass.equals(BluefloodTimerRollup.class)) {
            return Rollup.TimerFromTimer.compute(points);
        } else if (rollupTypeClass.equals(BluefloodGaugeRollup.class)) {
            return Rollup.GaugeFromGauge.compute(points);
        } else {
            throw new IOException(String.format("Unexpected rollup type: %s", rollupTypeClass.getSimpleName()));
        }
    }

    // TODO Add the multi search for historgrams
    protected MetricData getHistogramsByGranularity(String tenantId,
                                                   String metricName,
                                                   long from,
                                                   long to,
                                                   Granularity g) throws IOException {
        if (!g.isCoarser(Granularity.FULL)) {
            throw new IOException("Histograms are not available for this granularity");
        }

        final Timer.Context ctx = metricsFetchTimer.time();
        final Locator locator = Locator.createLocatorFromPathComponents(tenantId, metricName);

        MetricData data;
        try {
            data = AstyanaxReader.getInstance().getHistogramsForRange(locator, new Range(g.snapMillis(from), to), g);
            numHistogramPointsReturned.update(data.getData().getPoints().size());
        } finally {
            ctx.stop();
        }

        return data;
    }
}
