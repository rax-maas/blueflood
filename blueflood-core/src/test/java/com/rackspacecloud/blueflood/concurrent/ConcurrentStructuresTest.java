/*
 * Copyright 2013-2015 Rackspace
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class ConcurrentStructuresTest {
    private static final Logger logger = LoggerFactory.getLogger(ConcurrentStructuresTest.class);
    
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
}
