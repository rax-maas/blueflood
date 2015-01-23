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

package com.rackspacecloud.blueflood.concurrent;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadPoolBuilder {
    private static final Logger log = LoggerFactory.getLogger(ThreadPoolBuilder.class);
    /** Used to ensure that the thread pools have unique name. */
    private static final ConcurrentHashMap<String, AtomicInteger> nameMap = new ConcurrentHashMap<String, AtomicInteger>();
    private static final String DEFAULT_NAME = "Threadpool";
    private int corePoolSize = 10;
    private int maxPoolSize = 10;
    private int queueSize = 0;
    private TimeValue keepAliveTime = new TimeValue(30, TimeUnit.SECONDS);
    
    private RejectedExecutionHandler rejectedHandler = new ThreadPoolExecutor.CallerRunsPolicy();
    private Thread.UncaughtExceptionHandler exceptionHandler = new Thread.UncaughtExceptionHandler() {
        public void uncaughtException(Thread t, Throwable e) {
            log.error(e.getMessage(), e);
        }
    };

    private String threadNameFormat = null;
    private String poolName = null;

    public ThreadPoolBuilder() {
        withName(DEFAULT_NAME);
    }

    public ThreadPoolBuilder withCorePoolSize(int size) {
        this.corePoolSize = size;
        return this;
    }

    public ThreadPoolBuilder withMaxPoolSize(int size) {
        this.maxPoolSize = size;
        return this;
    }

    public ThreadPoolBuilder withSynchronousQueue() {
        this.queueSize = 0;
        return this;
    }

    public ThreadPoolBuilder withUnboundedQueue() {
        this.queueSize = -1;
        return this;
    }

    public ThreadPoolBuilder withBoundedQueue(int size) {
        this.queueSize = size;
        return this;
    }
    
    public ThreadPoolBuilder withKeepAliveTime(TimeValue time) {
        this.keepAliveTime = time;
        return this;
    }

    public ThreadPoolBuilder withRejectedHandler(RejectedExecutionHandler rejectedHandler) {
        this.rejectedHandler = rejectedHandler;
        return this;
    }

    /**
     * Set the threadpool name. Used to generate metric names and thread names.
     */
    public ThreadPoolBuilder withName(String name) {
        // ensure we've got a spot to put the thread id.
        if (!name.contains("%d")) {
            name = name + "-%d";
        }
        nameMap.putIfAbsent(name, new AtomicInteger(0));
        int id = nameMap.get(name).incrementAndGet();
        this.poolName = String.format(name, id);
        if (id > 1) {
            this.threadNameFormat = name.replace("%d", id + "-%d");
        } else {
            this.threadNameFormat = name;
        }
        return this;
    }

    public ThreadPoolBuilder withExceptionHandler(Thread.UncaughtExceptionHandler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
        return this;
    }

    public ThreadPoolExecutor build() {
        BlockingQueue<Runnable> workQueue;
        switch (this.queueSize) {
            case 0: workQueue = new SynchronousQueue<Runnable>();
                break;
            case -1: workQueue = new LinkedBlockingQueue<Runnable>();
                break;
            default: workQueue = new ArrayBlockingQueue<Runnable>(queueSize);
                break;
        };

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                corePoolSize, maxPoolSize,
                keepAliveTime.getValue(), keepAliveTime.getUnit(),
                workQueue,
                new ThreadFactoryBuilder().setNameFormat(threadNameFormat).setPriority(Thread.NORM_PRIORITY).setUncaughtExceptionHandler(exceptionHandler).build(),
                rejectedHandler);
        InstrumentedThreadPoolExecutor.instrument(executor, poolName);
        return executor;
    }
}