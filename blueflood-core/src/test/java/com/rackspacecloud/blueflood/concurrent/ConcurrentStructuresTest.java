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
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentStructuresTest {
    
    @Test
    public void testNoOpFutureNotifiesWithoutGetCall() throws InterruptedException {
        
        // blow up if someone accidentally puts a get() in this test.
        NoOpFuture<Integer> noop = new NoOpFuture<Integer>(42) {
            @Override
            public Integer get() throws InterruptedException, ExecutionException {
                throw new RuntimeException("get should never have been called");
            }

            @Override
            public Integer get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException, ExecutionException {
                throw new RuntimeException("get should never have been called");
            }
        };
        
        // should alredy have completed, even without getting.
        final CountDownLatch completionLatch = new CountDownLatch(1);
        noop.addListener(new Runnable() {
            public void run() {
                completionLatch.countDown();
            }
        }, Executors.newFixedThreadPool(1));
        
        Assert.assertTrue(completionLatch.await(1, TimeUnit.SECONDS));
        Assert.assertEquals(0, completionLatch.getCount());
    }

    @Test
    public void testThreadPoolsWithSameNameIsNotBad() throws InterruptedException {
        ThreadPoolExecutor svc1 = new ThreadPoolBuilder().withName("foo").build();
        ThreadPoolExecutor svc2 = new ThreadPoolBuilder().withName("foo").build();

        final List<String> threadNames = new ArrayList<String>();
        final CountDownLatch latch = new CountDownLatch(2);
        final CountDownLatch syncLatch = new CountDownLatch(1);

        Runnable first = new Runnable() {
            public void run() {
                threadNames.add(Thread.currentThread().getName());
                try {
                    // Waits a max of 1 sec to let the other thread to run.
                    // Counts down 'latch' only if other thread runs.
                    if (syncLatch.await(1000, TimeUnit.MILLISECONDS)) {
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    Assert.fail("Not a valid test anymore as one thread is interrupted.");
                }
            }
        };

        Runnable second = new Runnable() {
            public void run() {
                syncLatch.countDown(); // Tells the other thread that this one ran.
                threadNames.add(Thread.currentThread().getName());
                latch.countDown();
            }
        };

        svc1.submit(first);
        svc2.submit(second);

        // Verify both the threads ran to completion.
        Assert.assertTrue(latch.await(1000, TimeUnit.MILLISECONDS));

        Assert.assertEquals(2, threadNames.size());
        Assert.assertTrue(threadNames.contains("foo-0"));
        Assert.assertTrue(threadNames.contains("foo-2-0"));
    }

    @Test
    public void testAsyncFunctionSemantics() throws Exception {
        ThreadPoolBuilder poolBuilder = new ThreadPoolBuilder().withName("semantics");
        AsyncFunctionWithThreadPool<String, Integer> function = new AsyncFunctionWithThreadPool<String, Integer>(poolBuilder.build()) {
            @Override
            public ListenableFuture<Integer> apply(final String input) throws Exception {
                return getThreadPool().submit(new Callable<Integer>() {
                    public Integer call() throws Exception {
                        return Integer.parseInt(input);
                    }
                });
            }
        };
        
        Assert.assertEquals(new Integer(42), function.apply("42").get());
    }
    
    @Test
    public void testChainFinishesAsynchronousWork() throws Exception {
        ThreadPoolBuilder poolBuilder = new ThreadPoolBuilder().withCorePoolSize(5).withMaxPoolSize(5);
        
        // should get hit twice per task. we'll do two tasks, so 4x total.
        final CountDownLatch asyncLatch = new CountDownLatch(4);
        
        AsyncFunctionWithThreadPool<Integer, String> itos = new AsyncFunctionWithThreadPool<Integer, String>(poolBuilder.withName("itos").build()) {
            @Override
            public ListenableFuture<String> apply(final Integer input) throws Exception {
                return getThreadPool().submit(new Callable<String>() {
                    public String call() throws Exception {
                        Thread.sleep(200);
                        return Integer.toString(input + 1);
                    }
                });
            }
        };
        
        AsyncFunctionWithThreadPool<Integer, Integer> async = new AsyncFunctionWithThreadPool<Integer, Integer>(poolBuilder.withName("async").build()) {
            @Override
            public ListenableFuture<Integer> apply(Integer input) throws Exception {
                // pretend do do some busy work on the threadpool, but return immediately.
                getThreadPool().submit(new Runnable() {
                    public void run() {
                        try { 
                            Thread.sleep(1500);
                            asyncLatch.countDown();
                        } catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                });
                
                return new NoOpFuture<Integer>(input);
            }
        };
        
        AsyncFunctionWithThreadPool<String, Integer> stoi = new AsyncFunctionWithThreadPool<String, Integer>(poolBuilder.withName("stoi").build()) {
            @Override
            public ListenableFuture<Integer> apply(final String input) throws Exception {
                return getThreadPool().submit(new Callable<Integer>() {
                    public Integer call() throws Exception {
                        Thread.sleep(200);
                        return Integer.parseInt(input) + 1;
                    }
                });
            }
        };     
        
        // set up a chain that does 4 sync transformations + 2 async transformations.  Each chain should take ~800ms online + 3000 offline.
        final AsyncChain<Integer, Integer> chain = new AsyncChain<Integer, Integer>()
                .withFunction(itos)
                .withFunction(stoi)
                .withFunction(async)
                .withFunction(itos)
                .withFunction(stoi)
                .withFunction(async);
                
        // integer gets incremented by 1 for each non-async stage. so for input n, we should get n+4 out.
        
        // send two things through. since the pools can handle >1 thread, we should get answers at about the same time.
        final int n1 = 5;
        final int m1 = 10;
        
        final CountDownLatch incrementLatch = new CountDownLatch(2);
        final AtomicInteger result = new AtomicInteger(0);
        
        new Thread(new Runnable() {
            public void run() {
                try {
                    int tn1 = chain.apply(n1).get();
                    while (tn1 > 0) {
                        result.incrementAndGet();
                        tn1 -= 1;
                    }
                    incrementLatch.countDown();
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        }).start();
        
        new Thread(new Runnable() {
            public void run() {
                try {
                    int tm1 = chain.apply(m1).get();
                    while (tm1 > 0) {
                        result.incrementAndGet();
                        tm1 -= 1;
                    }
                    incrementLatch.countDown();
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        }).start();
        
        // wait for sync tasks to compleate 200ms * 4 = about 800 ms.
        incrementLatch.await(2, TimeUnit.SECONDS);
        
        // result should be n1 + 4 + m1 + 4;
        Assert.assertEquals(n1 + m1 + 4 + 4, result.get());
        
        // there were 3800 ms of work on the async processor though. that should still be churning though.
        Assert.assertTrue(asyncLatch.getCount() > 0);
        
        Assert.assertTrue(asyncLatch.await(4, TimeUnit.SECONDS));
        
        // everything is done now.
    }
    
    @Test
    public void testChainFinishesAfterTimeout() throws Exception {
        ThreadPoolBuilder poolBuilder = new ThreadPoolBuilder().withCorePoolSize(5).withMaxPoolSize(5);
        final CountDownLatch latch = new CountDownLatch(2);
        
        AsyncFunctionWithThreadPool<String, String> simpleFunc = new AsyncFunctionWithThreadPool<String, String>(poolBuilder.withName("timeoutFunc1").build()) {
            @Override
            public ListenableFuture<String> apply(final String input) throws Exception {
                return getThreadPool().submit(new Callable<String>() {
                    public String call() throws Exception {
                        Thread.sleep(1000);
                        return input + ",f1";
                    }
                });
            }
        };
        
        AsyncFunctionWithThreadPool<String, String> latchedFunc = new AsyncFunctionWithThreadPool<String, String>(poolBuilder.withName("timeoutFunc2").build()) {
            @Override
            public ListenableFuture<String> apply(final String input) throws Exception {
                return getThreadPool().submit(new Callable<String>() {
                    public String call() throws Exception {
                        latch.countDown();
                        return input + ",f2";
                    }
                });
            }
        };
        
        final AsyncChain<String, String> chain = new AsyncChain<String, String>()
                .withFunction(simpleFunc)
                .withFunction(simpleFunc)
                .withFunction(latchedFunc);
        
        // each pass through the chain should take about 2000ms.
        
        boolean timeoutHappened = false;
        try {
            String result = chain.apply("foo").get(500, TimeUnit.MILLISECONDS);
            Assert.fail("chain get should have failed");
        } catch (TimeoutException ex) {
            timeoutHappened = true;
        }
        Assert.assertTrue(timeoutHappened);
        
        timeoutHappened = false;
        try { 
            String result = chain.apply("bar").get(500, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {
            
        }
        
        // should pass without timing out.
        Assert.assertTrue(latch.await(3000, TimeUnit.MILLISECONDS));
        
        // Just for funsies, make sure we can actually get something.
        String result = chain.apply("baz").get(2500, TimeUnit.MILLISECONDS);
        Assert.assertEquals("baz,f1,f1,f2", result);
    }
    
    // verifies that the AsyncChain interface works with a mixture of simple AsyncFunctions (google) and 
    // AsyncFunctionWithThreadPool (our own concoction).
    @Test
    public void testMixingNoThreadpools() throws Exception {
        ThreadPoolBuilder poolBuilder = new ThreadPoolBuilder().withCorePoolSize(5).withMaxPoolSize(5);
        
        AsyncFunctionWithThreadPool<String, String> concat = new AsyncFunctionWithThreadPool<String, String>(poolBuilder.withName("concat").build()) {
            @Override
            public ListenableFuture<String> apply(final String input) throws Exception {
                return getThreadPool().submit(new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        return input + input;
                    }
                });
            }
        };
        
        AsyncFunction<String, String> plusOne = new AsyncFunction<String, String>() {
            @Override
            public ListenableFuture<String> apply(String input) throws Exception {
                return new NoOpFuture<String>(Integer.toString(Integer.parseInt(input) + 1));
            }
        };
        
        AsyncChain<String, String> chain = new AsyncChain<String, String>()
                .withFunction(concat)
                .withFunction(plusOne)
                .withFunction(concat) 
                .withFunction(plusOne);
        
        // 1 -> 11 -> 12 -> 1212 -> 1213
        String result = chain.apply("1").get(1000, TimeUnit.SECONDS);
        Assert.assertEquals("1213", result);
    }
    
    // Demonstrates a weakness in the chaining interface. We have no way to verify that inputs and outputs match
    // their parameterized types.
    @Test(expected = ClassCastException.class)
    public void testMismatchedTypes() throws Exception {
        
        // String -> Integer.
        AsyncFunction<String, Integer> stringToInt = new AsyncFunction<String, Integer>() {
            @Override
            public ListenableFuture<Integer> apply(String input) throws Exception {
                return new NoOpFuture<Integer>(Integer.parseInt(input));
            }
        };
        
        // Integer -> String
        AsyncFunction<Integer, String> intToString = new AsyncFunction<Integer, String>() {
            @Override
            public ListenableFuture<String> apply(Integer input) throws Exception {
                return new NoOpFuture<String>(Integer.toString(input));
            }
        };
        
        // String -> Integer plus Integer -> String means outputs are String.  But this instance declares that outputs 
        // are Integer.
        AsyncChain<String, Integer> chain = new AsyncChain<String, Integer>()
                .withFunction(stringToInt)
                .withFunction(intToString);
        
        ListenableFuture<Integer> future = chain.apply("1");
        // exception gets thrown on next line.
        Integer wrongTypeResult = future.get(1, TimeUnit.SECONDS);
    }
    
}
