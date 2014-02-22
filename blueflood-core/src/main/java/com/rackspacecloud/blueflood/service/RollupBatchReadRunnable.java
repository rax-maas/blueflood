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
import com.rackspacecloud.blueflood.exceptions.GranularityException;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RollupBatchReadRunnable<T extends Rollup> implements Runnable {
    private final RollupExecutionContext executionContext;
    private final List<SingleRollupReadContext> readContexts;
    private final RollupBatchWriter rollupBatchWriter;
    private static final Logger log = LoggerFactory.getLogger(RollupBatchReadRunnable.class);
    private static final Histogram rollupsPerBatch = Metrics.histogram(RollupService.class, "Rollups Read Per Batch");
    private static final Timer batchReadTimer = Metrics.timer(RollupService.class, "Rollup Batch Read");
    private Rollup.Type rollupComputer;
    private Range range;
    private Class<T> rollupClass;
    private CassandraModel.MetricColumnFamily cf;
    private CassandraModel.MetricColumnFamily dstCF;

    public RollupBatchReadRunnable(List<SingleRollupReadContext> readContexts, RollupExecutionContext context, RollupBatchWriter writer) {
        // All read contexts MUST be identical besides the locator
        this.readContexts = readContexts;
        this.executionContext = context;
        this.rollupBatchWriter = writer;
    }

    @Override
    public void run() {
        try {
            validateAndSetState();
        } catch (RuntimeException e) {
            log.error("Error with rollup read batch.", e);
            executionContext.decrementReadCounter((long)readContexts.size());
            throw e;
        }

        Timer.Context ctx = batchReadTimer.time();

        ArrayList<Locator> locators = new ArrayList<Locator>() {{
            for (SingleRollupReadContext context : readContexts) {
                add(context.getLocator());
            }
        }};

        try {
            Map<Locator, Points<T>> data = AstyanaxReader.getInstance().getDataToRoll(rollupClass, locators, range, cf);
            // next, compute the rollups.
            for (SingleRollupReadContext context : readContexts) {
                Rollup rollup = rollupComputer.compute(data.get(context.getLocator()));
                rollupBatchWriter.enqueueRollupForWrite(new SingleRollupWriteContext(rollup, context, dstCF));
            }
        } catch (RuntimeException e) {
            executionContext.markUnsuccessful(e);
            stopTimers(readContexts);
        } catch (IOException e) {
            executionContext.markUnsuccessful(e);
            stopTimers(readContexts);
        }
        executionContext.decrementReadCounter((long)readContexts.size());
        rollupsPerBatch.update(readContexts.size());
        RollupService.lastRollupTime.set(System.currentTimeMillis());
        ctx.stop();
    }

    private void validateAndSetState() throws RuntimeException {
        if (readContexts.size() == 0) throw new RuntimeException("Invalid count of readContexts of 0");
        SingleRollupReadContext readContext = readContexts.get(0);

        if (readContext.getRollupType() == null || readContext.getRollupClass() == null) {
            throw new RuntimeException("Rollup type/class found as null. RollupPreRead should have set this to a value for context: " + readContext);
        }

        Granularity rollupGranularity = readContext.getRollupGranularity();
        Granularity sourceGranularity;
        try {
            sourceGranularity = rollupGranularity.finer();
        } catch (GranularityException e) {
            throw new RuntimeException("Cannot rollup to " + rollupGranularity + " as it has no finer source data", e);
        }

        rollupClass = (Class<T>) readContext.getRollupClass();
        range = readContext.getRange();
        cf = CassandraModel.getColumnFamily(rollupClass, sourceGranularity);
        dstCF = CassandraModel.getColumnFamily(rollupClass, rollupGranularity);
        rollupComputer = getRollupComputer(readContext.getRollupType(), sourceGranularity);
    }

    private static void stopTimers(List<SingleRollupReadContext> contexts) {
        // Stops all timers that are tracking end-to-end rollup execution times.
        for (SingleRollupReadContext context : contexts) {
            context.getExecuteTimerContext().stop();
        }
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
