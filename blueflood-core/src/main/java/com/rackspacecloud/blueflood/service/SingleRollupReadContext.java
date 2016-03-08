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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Range;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.rollup.Granularity;

import java.util.HashMap;

/**
 * This class keeps track of what is happening in an rollup for a specific metric.
 */
public class SingleRollupReadContext {
    private final Locator locator;
    private final Range range;
    private static final Timer executeTimer = Metrics.timer(RollupService.class, "Rollup Execution Timer");
    private static final Histogram waitHist = Metrics.histogram(RollupService.class, "Rollup Wait Histogram");
    private static HashMap<Granularity, Meter> granToEnumMeters = new HashMap<Granularity, Meter>();
    static {
        for (Granularity rollupGranularity : Granularity.rollupGranularities()) {
            granToEnumMeters.put(rollupGranularity, Metrics.meter(RollupService.class, String.format("%s Enum Metrics Rolled up", rollupGranularity.shortName())));
        }
    }

    // documenting that this represents the DESTINATION granularity, not the SOURCE granularity.
    private final Granularity rollupGranularity;

    public SingleRollupReadContext(Locator locator, Range rangeToRead, Granularity rollupGranularity) {
        this.locator = locator;
        this.range = rangeToRead;
        this.rollupGranularity = rollupGranularity;
    }
    
    Timer getExecuteTimer() {
        return executeTimer;
    }

    Histogram getWaitHist() {
        return waitHist;
    }

    Meter getEnumMetricsMeterForGranularity(Granularity g) { return granToEnumMeters.get(g); }

    Granularity getRollupGranularity() {
        return this.rollupGranularity;
    }

    Range getRange() {
        return this.range;
    }

    Locator getLocator() {
        return this.locator;
    }
}
