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

package com.rackspacecloud.blueflood.utils;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricsRegistry;

public class RestartGauge extends Gauge<Integer> {
    boolean sentVal = false;

    @Override
    public Integer value() {
        // Sends a value of 1 for the first flush after service restart and Gauge instantiation, then zero.
        if (!sentVal){
            sentVal = true;
            return 1;
        }
        return 0;
    }

    public RestartGauge(MetricsRegistry registry, Class klass) {
        registry.newGauge(klass, "Restart", this);
    }

    public RestartGauge(Class klass) {
        this(Metrics.defaultRegistry(), klass);
    }

}