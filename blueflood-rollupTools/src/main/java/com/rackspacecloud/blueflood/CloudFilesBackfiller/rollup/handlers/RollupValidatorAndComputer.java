/*
 * Copyright 2014 Rackspace
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
package com.rackspacecloud.blueflood.CloudFilesBackfiller.rollup.handlers;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.rackspacecloud.blueflood.CloudFilesBackfiller.service.OutOFBandRollup;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.RollupBatchWriter;
import com.rackspacecloud.blueflood.service.RollupRunnable;
import com.rackspacecloud.blueflood.service.SingleRollupWriteContext;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class RollupValidatorAndComputer implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(RollupGenerator.class);
    private static Timer rollupTimer = Metrics.timer(RollupGenerator.class, "Rollup validator and calculator");
    private static Counter rollupValidationAndComputeFailed = Metrics.counter(RollupGenerator.class, "Failed to validate and calculate rollups");

    Locator loc;
    Range range;
    Points points;
    RollupBatchWriter writer;

    public RollupValidatorAndComputer(Locator loc, Range range, Points points, RollupBatchWriter writer) {
        this.loc = loc;
        this.range = range;
        this.points = points;
        this.writer = writer;
    }

    boolean hasAllZeroData(MetricData dataPoints) {
        boolean allZeroFlag = true;
        // Points should be of type BasicRollup. Will throw an exception if they are not.
        Map<Long, Points.Point<BasicRollup>> points = dataPoints.getData().getPoints();

        for (Map.Entry<Long, Points.Point<BasicRollup>> entry : points.entrySet()) {
            BasicRollup basicRollup = entry.getValue().getData();

            if((basicRollup.getMaxValue().isFloatingPoint() ? basicRollup.getMaxValue().toDouble() != 0.0 : basicRollup.getMaxValue().toLong() != 0) &&
                    (basicRollup.getMinValue().isFloatingPoint() ? basicRollup.getMinValue().toDouble() != 0.0 : basicRollup.getMinValue().toLong() != 0) &&
                    (basicRollup.getAverage().isFloatingPoint() ? basicRollup.getAverage().toDouble() != 0.0 : basicRollup.getAverage().toLong() != 0)) {
                allZeroFlag = false;
                break;
            }
        }
        return allZeroFlag;
    }

    @Override
    public void run() {
        Timer.Context rollupTimerContext = rollupTimer.time();
        try {
            Rollup.Type rollupComputer = RollupRunnable.getRollupComputer(RollupType.BF_BASIC, Granularity.FULL);
            Rollup rollup = rollupComputer.compute(points);
            writer.enqueueRollupForWrite(new SingleRollupWriteContext(rollup, loc, Granularity.MIN_5, CassandraModel.CF_METRICS_5M, range.getStart()));
            log.info("Calculated and queued rollup for "+loc+" within range "+range);
        } catch (Exception e) {
            // I want to be very harsh with exceptions encountered while validating and computing rollups. Just stop everything.
            log.error("Error encountered while validating and calculating rollups", e);
            rollupValidationAndComputeFailed.inc();
            RollupGenerator.rollupExecutors.shutdownNow();
            OutOFBandRollup.getRollupGeneratorThread().interrupt();
            // Stop the monitoring thread
            OutOFBandRollup.getMonitoringThread().interrupt();
            // Stop the file handler thread pool from sending data to buildstore
            FileHandler.handlerThreadPool.shutdownNow();
            throw new RuntimeException(e);
        } finally {
            rollupTimerContext.stop();
        }
    }
}
