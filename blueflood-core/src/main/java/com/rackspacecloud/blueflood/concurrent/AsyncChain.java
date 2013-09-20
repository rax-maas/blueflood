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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.ArrayList;
import java.util.List;

/**
 * A light-weight abstraction that allows you to link AsyncFunction objects together.  apply() will end up invoking the
 * entire chain.  It is possible to create entire trees of functions with both synchronous and asynchronous parts.
 * 
 * todo: we need a way to validate that the first parameterized type on the first AsyncFunction is I and the last
 * parameterized type on the last AsyncFunction is O.
 */
public class AsyncChain<I, O> implements AsyncFunction<I, O> {
    private List<AsyncFunction> functions = new ArrayList<AsyncFunction>();
    
    public AsyncChain<I, O> withFunction(AsyncFunction func) {
        functions.add(func);
        return this;
    }
    
    public ListenableFuture<O> apply(I input) throws Exception {
        List<AsyncFunction> copy = new ArrayList<AsyncFunction>(functions);
        AsyncFunction func;
        ListenableFuture nextInput = new NoOpFuture<I>(input);
        
        while (copy.size() > 0) {
            func = copy.remove(0);
            if (func instanceof AsyncFunctionWithThreadPool) {
                nextInput = Futures.transform(nextInput, func, ((AsyncFunctionWithThreadPool)func).getThreadPool());    
            } else {
                nextInput = Futures.transform(nextInput, func, MoreExecutors.sameThreadExecutor());
            }
        }
        return nextInput;
    }
}
