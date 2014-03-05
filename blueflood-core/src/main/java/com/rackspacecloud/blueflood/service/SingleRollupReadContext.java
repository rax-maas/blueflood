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
import com.codahale.metrics.Timer;
import com.rackspacecloud.blueflood.exceptions.GranularityException;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Range;
import com.rackspacecloud.blueflood.types.Rollup;
import com.rackspacecloud.blueflood.types.RollupType;
import com.rackspacecloud.blueflood.utils.Metrics;

/**
 * This class keeps track of what is happening in an rollup for a specific metric.
 */
public class SingleRollupReadContext {
    private final Locator locator;
    private final Range range;
    private static final Timer endToEndTimer = Metrics.timer(RollupService.class, "Rollup Execution Timer");
    private static final Histogram waitHist = Metrics.histogram(RollupService.class, "Rollup Wait Histogram");
    
    // documenting that this represents the DESTINATION granularity, not the SOURCE granularity.
    private final Granularity rollupGranularity;
    private final Granularity srcGranularity;
    private RollupType rollupType;
    private Class<? extends Rollup> rollupClass;
    private Timer.Context timingContext = null;

    public SingleRollupReadContext(Locator locator, Range rangeToRead, Granularity rollupGranularity) throws GranularityException {
        this.locator = locator;
        this.range = rangeToRead;
        this.rollupGranularity = rollupGranularity;
        this.srcGranularity = rollupGranularity.finer();
    }

    Timer.Context getExecuteTimerContext() {
        // This is pretty hacky, the way we pass this around. tracks end-to-end rollup execution time.
        if (timingContext == null) {
            timingContext = endToEndTimer.time();
        }
        return timingContext;
    }

    Histogram getWaitHist() {
        return waitHist;
    }

    Granularity getRollupGranularity() {
        return this.rollupGranularity;
    }

    Range getRange() {
        return this.range;
    }

    Locator getLocator() {
        return this.locator;
    }

    void setRollupType(RollupType rollupType) {
        this.rollupType = rollupType;
        this.rollupClass = RollupType.classOf(rollupType, rollupGranularity);
    }

    public Class<? extends Rollup> getRollupClass() {
        return rollupClass;
    }


    public Granularity getSourceGranularity() {
        return srcGranularity;
    }

    public RollupType getRollupType() {
        return rollupType;
    }
}
