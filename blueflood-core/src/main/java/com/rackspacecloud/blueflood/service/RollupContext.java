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

import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Range;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Timer;

import java.util.concurrent.TimeUnit;

/**
 * This class keeps track of what is happening in an rollup for a specific metric.
 */
class RollupContext {
    private final Locator locator;
    private final Range range;
    private final Granularity sourceGranularity;
    private static final Timer executeTimer = Metrics.newTimer(RollupService.class, "Rollup Execution Timer", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private static final Histogram waitHist = Metrics.newHistogram(RollupService.class, "Rollup Wait Histogram", true);

    RollupContext(Locator locator, Range rangeToRead, Granularity sourceGranularity) {
        this.locator = locator;
        this.range = rangeToRead;
        this.sourceGranularity = sourceGranularity;
    }
    
    Timer getExecuteTimer() {
        return executeTimer;
    }

    Histogram getWaitHist() {
        return waitHist;
    }

    Granularity getSourceGranularity() {
        return this.sourceGranularity;
    }

    Range getRange() {
        return this.range;
    }

    Locator getLocator() {
        return this.locator;
    }
}
