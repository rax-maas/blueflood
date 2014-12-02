package com.rackspacecloud.blueflood.concurrent;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.rackspacecloud.blueflood.utils.Metrics;

import java.util.concurrent.ThreadPoolExecutor;

public class InstrumentedThreadPoolExecutor {
    /**
     * Given a {@link ThreadPoolExecutor}, attach various {@link Gauge}s against its monitoring
     * properties.
     * @param executor {@link ThreadPoolExecutor} to monitor.
     * @param threadPoolName a unique name for this thread pool.
     */
    public static void instrument(final ThreadPoolExecutor executor, String threadPoolName) {
        MetricRegistry registry = Metrics.getRegistry();
        registry.register(name(threadPoolName, "queue-size"), new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return executor.getQueue().size();
            }
        });
        registry.register(name(threadPoolName, "queue-max"), new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return executor.getQueue().size() + executor.getQueue().remainingCapacity();
            }
        });
        registry.register(name(threadPoolName, "threadpool-active"), new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return executor.getActiveCount();
            }
        });
        registry.register(name(threadPoolName, "threadpool-max"), new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return executor.getMaximumPoolSize();
            }
        });
    }

    static String name(String threadPoolName, String suffix) {
        return MetricRegistry.name(InstrumentedThreadPoolExecutor.class, threadPoolName, suffix);
    }
}
