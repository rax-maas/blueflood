package com.rackspacecloud.blueflood.concurrent;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

        this.workQueueSize = Metrics.newGauge(InstrumentedThreadPoolExecutor.class, name, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return workQueue.size();
            }
        });
    }
}
