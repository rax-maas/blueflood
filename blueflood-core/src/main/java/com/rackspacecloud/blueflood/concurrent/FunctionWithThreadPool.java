/*
 * Copyright 2015 Rackspace
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

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * Does asynchronous work using a specified threadpool.
 * @param <I> function input type.
 * @param <O> function output type.
 */
public abstract class FunctionWithThreadPool<I, O> {
    private final ThreadPoolExecutor executor;
    /** Wraps {@link #executor}.*/
    private final ListeningExecutorService listeningExecutor;
    private Logger log = LoggerFactory.getLogger(getClass());
    
    public FunctionWithThreadPool(ThreadPoolExecutor executor) {
        this.executor = executor;
        this.listeningExecutor = MoreExecutors.listeningDecorator(executor);
    }
    
    public FunctionWithThreadPool<I, O> withLogger(Logger log) {
        this.log = log;
        return this;
    }
    
    public ListeningExecutorService getThreadPool() { return listeningExecutor; }
    
    public Logger getLogger() { return log; }

    public abstract O apply(I input) throws Exception;
    
    public void setPoolSize(int size) {
        this.executor.setCorePoolSize(size);
        this.executor.setMaximumPoolSize(size);
    }
    
    public int getPoolSize() {
        return this.executor.getCorePoolSize();
    }
}
