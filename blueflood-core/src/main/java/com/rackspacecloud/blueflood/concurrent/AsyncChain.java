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
