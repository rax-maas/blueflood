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
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Rollup;
import com.rackspacecloud.blueflood.types.RollupType;

public class SingleRollupWriteContext {
    private final Rollup rollup;
    private final Locator locator;
    private final Long timestamp;
    private final ColumnFamily<Locator, Long> destinationCF;
    private final Granularity granularity;

    // public only for tests
    public SingleRollupWriteContext(Rollup rollup, Locator locator, Granularity granularity, ColumnFamily<Locator, Long> destCf, Long timestamp) {
        this.rollup = rollup;
        this.locator = locator;
        this.granularity = granularity;
        this.destinationCF = destCf;
        this.timestamp = timestamp;
    }

    public SingleRollupWriteContext(Rollup rollup, SingleRollupReadContext singleRollupReadContext, ColumnFamily<Locator, Long> dstCF) {
        this(rollup, singleRollupReadContext.getLocator(), singleRollupReadContext.getRollupGranularity(), dstCF, singleRollupReadContext.getRange().getStart());
    }

    public Rollup getRollup() {
        return rollup;
    }

    public Locator getLocator() {
        return locator;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public ColumnFamily<Locator, Long> getDestinationCF() {
        return destinationCF;
    }
    
    public Granularity getGranularity() { return granularity; }
}
