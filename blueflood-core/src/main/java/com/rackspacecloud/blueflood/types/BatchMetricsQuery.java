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

package com.rackspacecloud.blueflood.types;

import com.rackspacecloud.blueflood.rollup.Granularity;

import java.util.List;

public class BatchMetricsQuery {
    private final List<Locator> locators;
    private final Range range;
    private final Granularity granularity;

    public BatchMetricsQuery(List<Locator> metrics, Range range, Granularity gran) {
        this.locators = metrics;
        this.range = range;
        this.granularity = gran;
    }

    public List<Locator> getLocators() {
        return locators;
    }

    public Range getRange() {
        return range;
    }

    public Granularity getGranularity() {
        return granularity;
    }
}
