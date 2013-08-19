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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;

public class AsyncChain<I, O> {
    private List<AsyncFunctionWithThreadPool> functions = new ArrayList<AsyncFunctionWithThreadPool>();
    
    public AsyncChain<I, O> withFunction(AsyncFunctionWithThreadPool func) {
        functions.add(func);
        return this;
    }
    
    public ListenableFuture<O> apply(I input) throws Exception {
        List<AsyncFunctionWithThreadPool> copy = new ArrayList<AsyncFunctionWithThreadPool>(functions);
        AsyncFunctionWithThreadPool func;
        ListenableFuture nextInput = new NoOpFuture<I>(input);
        
        while (copy.size() > 0) {
            func = copy.remove(0);
            nextInput = Futures.transform(nextInput, func, func.getThreadPool());
        }
        return nextInput;
    }
}
