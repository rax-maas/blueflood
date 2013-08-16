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

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * Does asynchronous work using a specified threadpool.
 * @param <I>
 * @param <O>
 */
public abstract class AsyncFunctionWithThreadPool<I, O> implements AsyncFunction<I, O> {
    
    private final ThreadPoolExecutor executor;
    // listieningExecutor wraps the above executor.
    private final ListeningExecutorService listeningExecutor;
    
    private Logger log = LoggerFactory.getLogger(getClass());
    
    public AsyncFunctionWithThreadPool(ThreadPoolExecutor executor) {
        this.executor = executor;
        this.listeningExecutor = MoreExecutors.listeningDecorator(executor);
    }
    
    public <I, O> AsyncFunctionWithThreadPool<I, O> withLogger(Logger log) {
        this.log = log;
        return (AsyncFunctionWithThreadPool<I,O>) this;
    }
    
    // todo: this method could be made uneeded if apply where implemented to always send work to this threadpool.
    // there would also need to be an abstract function that actually did the work asked for (transform I->O).
    public ListeningExecutorService getThreadPool() { return listeningExecutor; }
    public Logger getLogger() { return log; }

    public abstract ListenableFuture<O> apply(I input) throws Exception;
    
    public void setPoolSize(int size) {
        this.executor.setCorePoolSize(size);
        this.executor.setMaximumPoolSize(size);
    }
    
    public int getPoolSize() {
        return this.executor.getCorePoolSize();
    }
}
