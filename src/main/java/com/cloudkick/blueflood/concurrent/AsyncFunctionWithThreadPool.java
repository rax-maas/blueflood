package com.cloudkick.blueflood.concurrent;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * Does asynchronous work using a specified threadpool.
 * @param <I>
 * @param <O>
 */
public abstract class AsyncFunctionWithThreadPool<I, O> implements AsyncFunction<I, O> {
    private final ListeningExecutorService threadPool;
    
    private Logger log = LoggerFactory.getLogger(getClass());
    
    public AsyncFunctionWithThreadPool(ListeningExecutorService threadPool) {
        this.threadPool = threadPool;
    }
    
    public <I, O> AsyncFunctionWithThreadPool<I, O> withLogger(Logger log) {
        this.log = log;
        return (AsyncFunctionWithThreadPool<I,O>) this;
    }
    
    // todo: this method could be made uneeded if apply where implemented to always send work to this threadpool.
    // there would also need to be an abstract function that actually did the work asked for (transform I->O).
    public ListeningExecutorService getThreadPool() { return threadPool; }
    public Logger getLogger() { return log; }

    public abstract ListenableFuture<O> apply(I input) throws Exception;
}
