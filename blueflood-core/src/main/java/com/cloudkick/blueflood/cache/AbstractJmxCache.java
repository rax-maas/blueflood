package com.cloudkick.blueflood.cache;

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
