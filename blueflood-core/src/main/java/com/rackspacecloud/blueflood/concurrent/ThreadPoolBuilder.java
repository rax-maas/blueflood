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

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadPoolBuilder {
    private static final Logger log = LoggerFactory.getLogger(ThreadPoolBuilder.class);
    private static final Map<String, AtomicInteger> nameMap = new ConcurrentHashMap<String, AtomicInteger>();
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

    private String name = DEFAULT_NAME;

    public ThreadPoolBuilder() {

    }

    public ThreadPoolBuilder withCorePoolSize(int size) {
        this.corePoolSize = size;
        return this;
    }

    public ThreadPoolBuilder withMaxPoolSize(int size) {
        this.maxPoolSize = size;
        return this;
    }

    public ThreadPoolBuilder withUnboundedQueue() {
        this.queueSize = 0;
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

    public ThreadPoolBuilder withName(String name) {
        // ensure we've got a spot to put the thread id.
        if (name.indexOf("%d") < 0) {
            name = name + "-%d";
        }
        if (!nameMap.containsKey(name)) {
            nameMap.put(name, new AtomicInteger(2));
        } else {
            // unique threadpool names required for instrumentation
            name = name.replace("%d", "" + nameMap.get(name).getAndIncrement() + "-%d");
        }
        this.name = name;
        return this;

    }

    public ThreadPoolBuilder withExeptionHandler(Thread.UncaughtExceptionHandler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
        return this;
    }

    public ThreadPoolExecutor build() {
        if (name.equals(DEFAULT_NAME)) {
            this.withName(name); // causes pool to have a unique name
        }
        String metricName = name.subSequence(0, name.length() - 3) + " work queue size"; // don't need the '-%d'
        final BlockingQueue<Runnable> workQueue = this.queueSize > 0 ? new ArrayBlockingQueue<Runnable>(queueSize) :
                    new SynchronousQueue<Runnable>();

        return new InstrumentedThreadPoolExecutor(
                metricName,
                corePoolSize,
                maxPoolSize,
                keepAliveTime.getValue(),
                keepAliveTime.getUnit(),
                workQueue,
                new ThreadFactoryBuilder().setNameFormat(name).setPriority(Thread.NORM_PRIORITY).setUncaughtExceptionHandler(exceptionHandler).build(),
                rejectedHandler);
    }
}