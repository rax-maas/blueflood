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

package com.rackspacecloud.blueflood.cache;

import com.google.common.cache.CacheStats;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.util.JmxGauge;

import javax.management.ObjectName;

public abstract class AbstractJmxCache implements  CacheStatsMBean {

    private Gauge hitCount;
    private Gauge hitRate;
    private Gauge loadCount;
    private Gauge missRate;
    private Gauge requestCount;
    private Gauge totalLoadTime;

    public abstract CacheStats getStats();
    
    public long getHitCount() {
        return getStats().hitCount();
    }

    public double getHitRate() {
        return getStats().hitRate();
    }

    public long getMissCount() {
        return getStats().missCount();
    }

    public double getMissRate() {
        return getStats().missRate();
    }

    public long getLoadCount() {
        return getStats().loadCount();
    }

    public long getRequestCount() {
        return getStats().requestCount();
    }

    public long getTotalLoadTime() {
        return getStats().totalLoadTime();
    }

    public void instantiateYammerMetrics(Class klass, String scope, ObjectName nameObj) {
        hitCount = Metrics.newGauge(klass, "Hit Count", scope,
                new JmxGauge(nameObj, "HitCount"));
        hitRate = Metrics.newGauge(klass, "Hit Rate", scope,
                new JmxGauge(nameObj, "HitRate"));
        loadCount = Metrics.newGauge(klass, "Load Count", scope,
                new JmxGauge(nameObj, "LoadCount"));
        missRate = Metrics.newGauge(klass, "Miss Rate", scope,
                new JmxGauge(nameObj, "MissRate"));
        requestCount = Metrics.newGauge(klass, "Request Count", scope,
                new JmxGauge(nameObj, "RequestCount"));
        totalLoadTime = Metrics.newGauge(klass, "Total Load Time", scope,
                new JmxGauge(nameObj, "TotalLoadTime"));

    }
}
