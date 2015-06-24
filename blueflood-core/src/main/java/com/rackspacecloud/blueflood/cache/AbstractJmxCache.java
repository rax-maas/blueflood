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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.JmxAttributeGauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.CacheStats;
import com.rackspacecloud.blueflood.utils.Metrics;

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
        String name = MetricRegistry.name(klass);
        if (scope != null) {
            name = MetricRegistry.name(name, scope);
        }
        MetricRegistry reg = Metrics.getRegistry();
        hitCount = reg.register(MetricRegistry.name(name, "Hit Count"),
                new JmxAttributeGauge(nameObj, "HitCount"));
        hitRate = reg.register(MetricRegistry.name(name, "Hit Rate"),
                new JmxAttributeGauge(nameObj, "HitRate"));
        loadCount = reg.register(MetricRegistry.name(name, "Load Count"),
                new JmxAttributeGauge(nameObj, "LoadCount"));
        missRate = reg.register(MetricRegistry.name(name, "Miss Rate"),
                new JmxAttributeGauge(nameObj, "MissRate"));
        requestCount = reg.register(MetricRegistry.name(name, "Request Count"),
                new JmxAttributeGauge(nameObj, "RequestCount"));
        totalLoadTime = reg.register(MetricRegistry.name(name, "Total Load Time"),
                new JmxAttributeGauge(nameObj, "TotalLoadTime"));
    }
}
