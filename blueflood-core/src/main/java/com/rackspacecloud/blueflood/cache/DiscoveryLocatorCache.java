package com.rackspacecloud.blueflood.cache;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.rackspacecloud.blueflood.utils.Metrics;

import java.util.concurrent.TimeUnit;

/**
 * Separate locator cache for Discovery writes.
 */
public class DiscoveryLocatorCache {

    private static LocatorCache instance = new LocatorCache(10, TimeUnit.MINUTES,
            3, TimeUnit.DAYS);


    static {
        Metrics.getRegistry().register(MetricRegistry.name(DiscoveryLocatorCache.class, "Current Discovery Locators Count"),
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return instance.getCurrentLocatorCount();
                    }
                });

        Metrics.getRegistry().register(MetricRegistry.name(DiscoveryLocatorCache.class, "Current Delayed Discovery Locators Count"),
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return instance.getCurrentDelayedLocatorCount();
                    }
                });
    }


    public static LocatorCache getInstance() {
        return instance;
    }
}
