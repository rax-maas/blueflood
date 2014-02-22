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

package com.rackspacecloud.blueflood.service;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Metrics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by jburkhart on 2/22/14.
 */
public class RollupBatchReadRunnable<T extends Rollup> implements Runnable {
    private final RollupExecutionContext executionContext;
    private final ArrayList<SingleRollupReadContext> readContexts;
    private static final Histogram rollupsPerBatch = Metrics.histogram(RollupService.class, "Rollups Read Per Batch");
    private static final Timer batchReadTimer = Metrics.timer(RollupService.class, "Rollup Batch Read");

    public RollupBatchReadRunnable(ArrayList<SingleRollupReadContext> readContexts, RollupExecutionContext context) {
        this.readContexts = readContexts;
        this.executionContext = context;
    }


    @Override
    public void run() {
        if (readContexts.size() == 0) throw new RuntimeException("Invalid count of readContexts of 0");
        SingleRollupReadContext readContext = readContexts.get(0);
        Class<? extends Rollup> rollupClass = readContext.getRollupClass();
        Range range = readContext.getRange();
        CassandraModel.MetricColumnFamily cf = CassandraModel.getColumnFamily(rollupClass, readContext.getSourceGranularity());
        Rollup.Type rollupComputer = getRollupComputer(readContext.getRollupType(), readContext.getSourceGranularity());

        Timer.Context ctx = batchReadTimer.time();

        ArrayList<Locator> locators = new ArrayList<Locator>() {{
            for (SingleRollupReadContext context : readContexts) {
                add(context.getLocator());
            }
        }};

        try {
            Map<Locator, Points<? extends Rollup>> data = AstyanaxReader.getInstance().getDataToRoll(rollupClass, locators, range, cf);
            // next, compute the rollups.
            for (Map.Entry<Locator, Points<? extends Rollup>> locatorPointsEntry : data.entrySet()) {
                Rollup rollup = rollupComputer.compute(locatorPointsEntry.getValue());
            }

            rollup = getRollupComputer(rollupType, srcGran).compute(input);
        } catch (ConnectionException e) {
            executionContext.markUnsuccessful(e);
        } catch (IOException e) {
            executionContext.markUnsuccessful(e);
        }
        executionContext.decrementReadCounter(readContexts.size());
        rollupsPerBatch.update(readContexts.size());
        RollupService.lastRollupTime.set(System.currentTimeMillis());
        ctx.stop();
    }

    public static Rollup.Type getRollupComputer(RollupType srcType, Granularity srcGran) {
        switch (srcType) {
            case COUNTER:
                return Rollup.CounterFromCounter;
            case TIMER:
                return Rollup.TimerFromTimer;
            case GAUGE:
                return Rollup.GaugeFromGauge;
            case BF_HISTOGRAMS:
                return srcGran == Granularity.FULL ? Rollup.HistogramFromRaw : Rollup.HistogramFromHistogram;
            case BF_BASIC:
                return srcGran == Granularity.FULL ? Rollup.BasicFromRaw : Rollup.BasicFromBasic;
            case SET:
                return Rollup.SetFromSet;
            default:
                break;
        }
        throw new IllegalArgumentException(String.format("Cannot compute rollups for %s from %s", srcType.name(), srcGran.shortName()));
    }

}
