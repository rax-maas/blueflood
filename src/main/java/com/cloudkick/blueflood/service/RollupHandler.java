package com.cloudkick.blueflood.service;

import com.cloudkick.blueflood.io.AstyanaxReader;
import com.cloudkick.blueflood.rollup.Granularity;
import com.cloudkick.blueflood.types.Locator;
import com.cloudkick.blueflood.types.Range;
import com.cloudkick.blueflood.types.Rollup;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import telescope.thrift.*;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class RollupHandler implements RollupServer.Iface {

    private static final Logger log = LoggerFactory.getLogger(RollupHandler.class);

    private final Meter rollupsByPointsMeter = Metrics.newMeter(RollupHandler.class, "Get rollups by points", "BF-API", TimeUnit.SECONDS);
    private final Meter rollupsByGranularityMeter = Metrics.newMeter(RollupHandler.class, "Get rollups by gran", "BF-API", TimeUnit.SECONDS);
    private final Timer metricsForCheckTimer = Metrics.newTimer(RollupHandler.class, "Get metrics for check", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private final Timer metricsFetchTimer = Metrics.newTimer(RollupHandler.class, "Get metrics from db", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private final Timer rollupsCalcOnReadTimer = Metrics.newTimer(RollupHandler.class, "Rollups calculation on read", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private final Histogram numFullPointsReturned = Metrics.newHistogram(RollupHandler.class, "Full res points returned", true);
    private final Histogram numRollupPointsReturned = Metrics.newHistogram(RollupHandler.class, "Rollup points returned", true);

    public RollupHandler() {
    }

    public RollupMetrics GetRollupByPoints(
            String metricName, 
            long from,
            long to, 
            int points) throws TException {
        rollupsByPointsMeter.mark();
        Granularity g = Granularity.granularityFromPointsInInterval(from, to, points);
        return GetRollupByGranularity(metricName, from, to, g);
    }

    public RollupMetrics GetRollupByResolution(
            String metricName,
            long from, 
            long to, 
            Resolution resolution) throws TException {
        rollupsByGranularityMeter.mark();
        if (resolution == null)
          throw new TException("Resolution is not set");
        Granularity g = Granularity.granularities()[resolution.getValue()];
        return GetRollupByGranularity(metricName, from, to, g);
    }

    RollupMetrics GetRollupByGranularity(
            String metricName,
            long from,
            long to,
            Granularity g) throws TException {

        final TimerContext ctx = metricsFetchTimer.time();
        final Locator locator = new Locator(metricName);
        final List<RollupMetric> points = AstyanaxReader.getInstance().getDatapointsForRange(
                locator,
                new Range(g.snapMillis(from), to),
                g);

        // if Granularity is FULL, we are missing raw data - can't generate that
        if (g != Granularity.FULL && points != null) {
            final TimerContext rollupsCalcCtx = rollupsCalcOnReadTimer.time();

            long latest = from;
            for (RollupMetric p : points) {
                if (p.getTimestamp() > latest) {
                    latest = p.getTimestamp();
                }
            }

            // timestamp of the end of the latest slot
            if (latest + g.milliseconds() <= to) {

                // missing some rollups, generate more
                for (Range r : Range.rangesForInterval(g, latest + g.milliseconds(), to)) {
                    Rollup rollup = AstyanaxReader.getInstance().readAndCalculate(locator, r, Granularity.FULL);
                    if (rollup == null) {
                        // errant string metric, already logged during deserialization. log at error so someone goes
                        // and cleans these up.
                        log.error("Errant string metric " + metricName);
                    } else if (rollup.getCount() > 0) {
                        RollupMetric rm = new RollupMetric();
                        rm.setTimestamp(r.getStart());
                        points.add(AstyanaxReader.buildRollupThriftMetric(rm, rollup));
                    }
                }
            }
            rollupsCalcCtx.stop();
        }
        ctx.stop();

        if (g == Granularity.FULL) {
            numFullPointsReturned.update(points.size());
        } else {
            numRollupPointsReturned.update(points.size());
        }

        String unitString = AstyanaxReader.getUnitString(locator);

        return new RollupMetrics(points, unitString);
    }

    public List<MetricInfo> GetMetricsForCheck(String accountId, String entityId, String checkId) throws TException {
        final TimerContext ctx = metricsForCheckTimer.time();
        List<MetricInfo> metrics = AstyanaxReader.getInstance().getMetricsForCheck(accountId, entityId, checkId);
        ctx.stop();

        return metrics;
    }
}
