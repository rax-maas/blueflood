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

package com.rackspacecloud.blueflood.service;

import com.netflix.astyanax.model.ColumnFamily;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Range;
import com.rackspacecloud.blueflood.types.Rollup;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Timer;

import java.util.concurrent.TimeUnit;

/**
 * Will eventually become RollupContext as soon as the existing RollupContext is renamed to ScheduleContext.
 * This class keeps track of what is happening in an rollup for a specific metric.  Rollups for a single metric are run 
 * in parallel, as indicated by the counter.  The counter signals a thread that is waiting for all rollups for that
 * metric is finished so that the metric service can be signaled/
 */
class RollupContext {
    private final Locator locator;
    private final Range range;
    private final ColumnFamily<Locator, Long> srcCF; // this is the source column family to read from.
    private final ColumnFamily<Locator, Long> destCF; // this is the dest column family to write to.
    private static final Timer executeTimer = Metrics.newTimer(RollupService.class, "Rollup Execution Timer", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private static final Histogram waitHist = Metrics.newHistogram(RollupService.class, "Rollup Wait Histogram", true);

    RollupContext(Locator locator, Range rangeToRead, ColumnFamily srcColumnFamily, ColumnFamily destColumnFamily) {
        this.locator = locator;
        this.range = rangeToRead;
        this.srcCF = srcColumnFamily;
        this.destCF = destColumnFamily;
    }
    
    Timer getExecuteTimer() {
        return executeTimer;
    }

    Histogram getWaitHist() {
        return waitHist;
    }

    public ColumnFamily<Locator, Long> getSourceColumnFamily() {
        return this.srcCF;
    }

    public ColumnFamily<Locator, Long> getDestinationColumnFamily() {
        return this.destCF;
    }

    Range getRange() {
        return this.range;
    }

    Locator getLocator() {
        return this.locator;
    }
}
