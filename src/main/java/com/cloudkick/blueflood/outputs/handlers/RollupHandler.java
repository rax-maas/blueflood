package com.cloudkick.blueflood.outputs.handlers;

import com.cloudkick.blueflood.io.AstyanaxReader;
import com.cloudkick.blueflood.outputs.formats.RollupData;
import com.cloudkick.blueflood.rollup.Granularity;
import com.cloudkick.blueflood.types.Locator;
import com.cloudkick.blueflood.types.Points;
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

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RollupHandler {
    private static final Logger log = LoggerFactory.getLogger(ThriftRollupHandler.class);

    protected final Meter rollupsByPointsMeter = Metrics.newMeter(RollupHandler.class, "Get rollups by points", "BF-API", TimeUnit.SECONDS);
    protected final Meter rollupsByGranularityMeter = Metrics.newMeter(RollupHandler.class, "Get rollups by gran", "BF-API", TimeUnit.SECONDS);
    protected final Timer metricsForCheckTimer = Metrics.newTimer(RollupHandler.class, "Get metrics for check", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    protected final Timer metricsFetchTimer = Metrics.newTimer(RollupHandler.class, "Get metrics from db", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    protected final Timer rollupsCalcOnReadTimer = Metrics.newTimer(RollupHandler.class, "Rollups calculation on read", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    protected final Histogram numFullPointsReturned = Metrics.newHistogram(RollupHandler.class, "Full res points returned", true);
    protected final Histogram numRollupPointsReturned = Metrics.newHistogram(RollupHandler.class, "Rollup points returned", true);

    protected RollupData getRollupByGranularity(
            String accountId,
            String metricName,
            long from,
            long to,
            Granularity g) throws TException {

        final TimerContext ctx = metricsFetchTimer.time();
        final Locator locator = Locator.createLocatorFromPathComponents(accountId, metricName);
        final RollupData rollupData = AstyanaxReader.getInstance().getDatapointsForRange(
                locator,
                new Range(g.snapMillis(from), to),
                g);

        // if Granularity is FULL, we are missing raw data - can't generate that
        if (g != Granularity.FULL && rollupData != null) {
            final TimerContext rollupsCalcCtx = rollupsCalcOnReadTimer.time();

            long latest = from;
            Map<Long, Points.Point<Object>> points = rollupData.getData().getPoints();
            for (Map.Entry<Long, Points.Point<Object>> point : points.entrySet()) {
                if (point.getValue().getTimestamp() > latest) {
                    latest = point.getValue().getTimestamp();
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
                        rollupData.getData().add(new Points.Point<Rollup>(r.getStart(), rollup));
                    }
                }
            }
            rollupsCalcCtx.stop();
        }
        ctx.stop();

        if (g == Granularity.FULL) {
            numFullPointsReturned.update(rollupData.getData().getPoints().size());
        } else {
            numRollupPointsReturned.update(rollupData.getData().getPoints().size());
        }

        return rollupData;
    }
}
