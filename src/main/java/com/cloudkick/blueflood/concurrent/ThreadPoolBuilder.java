package com.cloudkick.blueflood.concurrent;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolBuilder {
    private static final Logger log = LoggerFactory.getLogger(ThreadPoolBuilder.class);
    
    private int corePoolSize = 10;
    private int maxPoolSize = 10;
    private BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>();
    private RejectedExecutionHandler rejectedHandler = new ThreadPoolExecutor.CallerRunsPolicy();
    private Thread.UncaughtExceptionHandler exceptionHandler = new Thread.UncaughtExceptionHandler() {
        public void uncaughtException(Thread t, Throwable e) {
            log.error(e.getMessage(), e);
        }
    }; 
    private String name = "Thread";
    
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
        this.workQueue = new LinkedBlockingQueue<Runnable>();
        return this;
    }
    
    public ThreadPoolBuilder withBoundedQueue(int size) {
        this.workQueue = new ArrayBlockingQueue<Runnable>(size);
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
        this.name = name;
        return this;
        
    }
    
    public ThreadPoolBuilder withExeptionHandler(Thread.UncaughtExceptionHandler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
        return this;
    }
    
    public ListeningExecutorService build() {
        return MoreExecutors.listeningDecorator(new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                30, TimeUnit.SECONDS, // hard code the timeout.
                workQueue,
                new ThreadFactoryBuilder().setNameFormat(name).setPriority(Thread.NORM_PRIORITY).setUncaughtExceptionHandler(exceptionHandler).build(),
                rejectedHandler));
    }
}