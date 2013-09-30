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

import com.rackspacecloud.blueflood.io.AstyanaxIO;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RollupHandler {
    private static final Logger log = LoggerFactory.getLogger(RollupHandler.class);

    protected final Meter rollupsByPointsMeter = Metrics.newMeter(RollupHandler.class, "Get rollups by points", "BF-API", TimeUnit.SECONDS);
    protected final Meter rollupsByGranularityMeter = Metrics.newMeter(RollupHandler.class, "Get rollups by gran", "BF-API", TimeUnit.SECONDS);
    protected final Timer metricsForCheckTimer = Metrics.newTimer(RollupHandler.class, "Get metrics for check", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    protected final Timer metricsFetchTimer = Metrics.newTimer(RollupHandler.class, "Get metrics from db", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    protected final Timer rollupsCalcOnReadTimer = Metrics.newTimer(RollupHandler.class, "Rollups calculation on read", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    protected final Histogram numFullPointsReturned = Metrics.newHistogram(RollupHandler.class, "Full res points returned", true);
    protected final Histogram numRollupPointsReturned = Metrics.newHistogram(RollupHandler.class, "Rollup points returned", true);

    protected MetricData getRollupByGranularity(
            String tenantId,
            String metricName,
            long from,
            long to,
            Granularity g) {

        final TimerContext ctx = metricsFetchTimer.time();
        final Locator locator = Locator.createLocatorFromPathComponents(tenantId, metricName);
        final MetricData metricData = AstyanaxReader.getInstance().getDatapointsForRange(
                locator,
                new Range(g.snapMillis(from), to),
                g);

        // if Granularity is FULL, we are missing raw data - can't generate that
        if (g != Granularity.FULL && metricData != null) {
            final TimerContext rollupsCalcCtx = rollupsCalcOnReadTimer.time();

            long latest = from;
            Map<Long, Points.Point> points = metricData.getData().getPoints();
            for (Map.Entry<Long, Points.Point> point : points.entrySet()) {
                if (point.getValue().getTimestamp() > latest) {
                    latest = point.getValue().getTimestamp();
                }
            }

            // timestamp of the end of the latest slot
            if (latest + g.milliseconds() <= to) {
                // missing some rollups, generate more (5 MIN rollups only)
                for (Range r : Range.rangesForInterval(g, latest + g.milliseconds(), to)) {
                    try {
                        
                        Points<SimpleNumber> dataToRoll = AstyanaxReader.getInstance().getSimpleDataToRoll(locator, r);
                        BasicRollup rollup = Rollup.BasicFromRaw.compute(dataToRoll);
                        if (rollup.getCount() > 0) {
                            metricData.getData().add(new Points.Point<BasicRollup>(r.getStart(), rollup));
                        }

                    } catch (IOException ex) {
                        log.error("Exception computing rollups during read: ", ex);
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
}
