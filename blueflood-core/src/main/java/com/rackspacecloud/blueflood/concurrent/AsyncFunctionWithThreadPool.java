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
import java.util.concurrent.ThreadPoolExecutor;
import org.slf4j.Logger;

/**
 * Does asynchronous work using a specified threadpool.
 * @param <I>
 * @param <O>
 */
public abstract class AsyncFunctionWithThreadPool<I, O> extends FunctionWithThreadPool<I, O> implements AsyncFunction<I, O> {
    public AsyncFunctionWithThreadPool(ThreadPoolExecutor executor) {
        super(executor);
    }
    public abstract ListenableFuture<O> apply(I input) throws Exception;
    @Override
    public <I, O> AsyncFunctionWithThreadPool<I, O> withLogger(Logger log) {
        this.log = log;
        return (AsyncFunctionWithThreadPool<I,O>) this;
    }
}
