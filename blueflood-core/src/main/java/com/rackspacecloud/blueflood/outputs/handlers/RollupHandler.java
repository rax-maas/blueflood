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
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.io.SearchResult;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

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
    protected final Timer metricsForCheckTimer = Metrics.timer(RollupHandler.class, "Get metrics for check");
    protected final Timer metricsFetchTimer = Metrics.timer(RollupHandler.class, "Get metrics from db");
    protected final Timer rollupsCalcOnReadTimer = Metrics.timer(RollupHandler.class, "Rollups calculation on read");
    protected final Histogram numFullPointsReturned = Metrics.histogram(RollupHandler.class, "Full res points returned");
    protected final Histogram numRollupPointsReturned = Metrics.histogram(RollupHandler.class, "Rollup points returned");
    protected final Histogram numHistogramPointsReturned = Metrics.histogram(RollupHandler.class, "Histogram points returned");

    private static final boolean ROLLUP_REPAIR = Configuration.getInstance().getBooleanProperty(CoreConfig.REPAIR_ROLLUPS_ON_READ);
    protected final DiscoveryIO discoveryHandler = loadDiscoveryModule();
    private static final ThreadPoolBuilder poolBuilder = new ThreadPoolBuilder().withName("MetricDataUnit Pool");
    private static final ExecutorService executorService =  poolBuilder.build();

    private static DiscoveryIO loadDiscoveryModule() {
        List<String> modules = Configuration.getInstance().getListProperty(CoreConfig.DISCOVERY_MODULES);

        if (!modules.isEmpty() && modules.size() != 1) {
            throw new RuntimeException("Cannot load query service with more than one discovery module");
        }

        ClassLoader classLoader = DiscoveryIO.class.getClassLoader();
        for (String module : modules) {
            log.info("Loading metric discovery module " + module);
            try {
                Class discoveryClass = classLoader.loadClass(module);
                log.info("Registering metric discovery module " + module);
                return (DiscoveryIO) discoveryClass.newInstance();
            } catch (InstantiationException e) {
                log.error("Unable to create instance of metric discovery class for: " + module, e);
            } catch (IllegalAccessException e) {
                log.error("Error starting metric discovery module: " + module, e);
            } catch (ClassNotFoundException e) {
                log.error("Unable to locate metric discovery module: " + module, e);
            } catch (RuntimeException e) {
                log.error("Error starting metric discovery module: " + module, e);
            } catch (Throwable e) {
                log.error("Error starting metric discovery module: " + module, e);
            }
        }

        return null;
    }

    protected MetricData getRollupByGranularity(
            String tenantId,
            String metricName,
            final long from,
            final long to,
            final Granularity g) {

        final Timer.Context ctx = metricsFetchTimer.time();
        final Locator locator = Locator.createLocatorFromPathComponents(tenantId, metricName);

        MetricData metricData = getMetricData(from, to, g, locator);

        boolean isRollable = metricData.getType().equals(MetricData.Type.NUMBER.toString())
                || metricData.getType().equals(MetricData.Type.HISTOGRAM.toString());

        // if Granularity is FULL, we are missing raw data - can't generate that
        if (ROLLUP_REPAIR && isRollable && g != Granularity.FULL && metricData != null) {
            final Timer.Context rollupsCalcCtx = rollupsCalcOnReadTimer.time();

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
            rollupsCalcCtx.stop();
        }
        ctx.stop();

        if (g == Granularity.FULL) {
            numFullPointsReturned.update(metricData.getData().getPoints().size());
        } else {
            numRollupPointsReturned.update(metricData.getData().getPoints().size());
        }

        return metricData;
    }

    private MetricData getMetricData(long from, long to, Granularity g, Locator locator) {

        String unit = null;
        Callable<String> unitsGetter = new UnitsGetter(locator.getTenantId(),locator.getMetricName());
        Future<String> unitsGetterFuture = executorService.submit(unitsGetter);

        MetricData metricData;
        metricData = AstyanaxReader.getInstance().getDatapointsForRange(
                locator,
                new Range(g.snapMillis(from), to),
                g);

        try {
            unit = unitsGetterFuture.get();
        } catch (InterruptedException ie) {
            log.info(ie.getMessage());
        } catch (ExecutionException ee) {
            log.info(ee.getMessage());
        }

        metricData.setUnit(unit);
        return metricData;
    }

    private List<Points.Point> repairRollupsOnRead(Locator locator, Granularity g, long from, long to) {
        List<Points.Point> repairedPoints = new ArrayList<Points.Point>();

        Iterable<Range> ranges = Range.rangesForInterval(g, g.snapMillis(from), to);
        for (Range r : ranges) {
            try {
                MetricData data = getMetricData(r.getStart(), r.getStop(), g, locator);
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
        } else if (rollupTypeClass.equals(CounterRollup.class)) {
            return Rollup.CounterFromCounter.compute(points);
        } else if (rollupTypeClass.equals(SetRollup.class)) {
            return Rollup.SetFromSet.compute(points);
        } else if (rollupTypeClass.equals(TimerRollup.class)) {
            return Rollup.TimerFromTimer.compute(points);
        } else if (rollupTypeClass.equals(GaugeRollup.class)) {
            return Rollup.GaugeFromGauge.compute(points);
        } else {
            throw new IOException(String.format("Unexpected rollup type: %s", rollupTypeClass.getSimpleName()));
        }
    }

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

    private class UnitsGetter implements Callable {
        private String tenantId;
        private String metricName;

        public UnitsGetter(String tenantId, String metricName) {
            this.tenantId = tenantId;
            this.metricName = metricName;
        }

        public String call() {
            String unit = null;
            try {
                List<SearchResult> results = discoveryHandler.search(tenantId, metricName);
                for (SearchResult res : results) {
                    unit = res.getUnit();
                    break;
                }
            } catch (Exception ex) {
                log.info(ex.getMessage());
            }
            return unit;
        }
    }
}
