package com.rackspacecloud.blueflood.cache;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.rackspacecloud.blueflood.utils.Metrics;

import java.util.concurrent.TimeUnit;

/**
 * Separate locator cache for Batch writes.
 */
public class BatchLocatorCache {

    private static LocatorCache instance = new LocatorCache(10, TimeUnit.MINUTES,
            3, TimeUnit.DAYS);


    static {
        Metrics.getRegistry().register(MetricRegistry.name(BatchLocatorCache.class, "Current Batch Locators Count"),
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return instance.getCurrentLocatorCount();
                    }
                });

        Metrics.getRegistry().register(MetricRegistry.name(BatchLocatorCache.class, "Current Delayed Batch Locators Count"),
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
