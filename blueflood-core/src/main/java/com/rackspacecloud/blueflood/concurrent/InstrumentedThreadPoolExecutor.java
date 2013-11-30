package com.rackspacecloud.blueflood.concurrent;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.rackspacecloud.blueflood.utils.Metrics;

import java.util.concurrent.*;

public class InstrumentedThreadPoolExecutor extends ThreadPoolExecutor {
    private final Gauge<Integer> workQueueSize;

    public InstrumentedThreadPoolExecutor(String name,
                                          int corePoolSize,
                                          int maximumPoolSize,
                                          long keepAliveTime,
                                          TimeUnit unit,
                                          final BlockingQueue<Runnable> workQueue,
                                          ThreadFactory threadFactory,
                                          RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
        this.workQueueSize = new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return workQueue.size();
            }
        };
        Metrics.getRegistry().register(MetricRegistry.name(InstrumentedThreadPoolExecutor.class, name), this.workQueueSize);
    }
}
