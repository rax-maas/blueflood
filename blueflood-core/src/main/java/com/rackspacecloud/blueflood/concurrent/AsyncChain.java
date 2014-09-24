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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
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
 * <p>
 *     Sample usage:
 * <pre>
 * <code>func = AsyncChain.withFunction(aToB).withFunction(bToC).withFunction(cToD).build();
 * func.apply(a) // returns a future of D.
 * </code>
 * </p>
 */
public class AsyncChain<I, O> implements AsyncFunction<I, O> {
    private final ImmutableList<AsyncFunction<?, ?>> functions;

    public AsyncChain(List<AsyncFunction<?, ?>> functions) {
        this.functions = ImmutableList.copyOf(functions);
    }

    public ListenableFuture<O> apply(I input) throws Exception {
        ListenableFuture nextInput = new NoOpFuture<I>(input);
        for (AsyncFunction func : functions) {
            if (func instanceof AsyncFunctionWithThreadPool) {
                nextInput = Futures.transform(nextInput, func, ((AsyncFunctionWithThreadPool)func).getThreadPool());
            } else {
                nextInput = Futures.transform(nextInput, func, MoreExecutors.sameThreadExecutor());
            }
        }
        return nextInput;
    }

    /**
     * Start the builder for a chain of async functions.
     */
    public static <A, B> Builder<A, B> withFunction(AsyncFunction<A, B> function) {
        return new Builder<A, B>(function);
    }

    public static class Builder <A, B> {
        private final List<AsyncFunction<?, ?>> functions;

        private Builder(AsyncFunction<A, B> function) {
            Preconditions.checkNotNull(function);
            functions = new ArrayList<AsyncFunction<?, ?>>();;
            functions.add(function);
        }

        private Builder(List<AsyncFunction<?, ?>> functions) {
            Preconditions.checkNotNull(functions);
            this.functions = functions;
        }

        public <C> Builder<A, C> withFunction(AsyncFunction<B, C> function) {
            Preconditions.checkNotNull(function);
            ArrayList<AsyncFunction<?, ?>> copy = new ArrayList<AsyncFunction<?, ?>>(this.functions);
            copy.add(function);
            return new Builder<A, C>(copy);
        }

        public AsyncChain<A, B> build() {
            return new AsyncChain<A, B>(functions);
        }
    }
}
