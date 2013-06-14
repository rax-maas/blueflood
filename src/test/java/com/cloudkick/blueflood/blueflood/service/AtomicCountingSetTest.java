package com.cloudkick.blueflood.service;


import com.cloudkick.blueflood.utils.AtomicCountingSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.*;

public class AtomicCountingSetTest {
    private AtomicCountingSet<Integer> testSet;

    @Before
    public void setUp() {
        testSet = new AtomicCountingSet<Integer>();
    }

    @Test
    public void testSimultaneousPut() throws Exception {
        ExecutorService executorService = Executors.newCachedThreadPool();

        final CountDownLatch startLatch = new CountDownLatch(1);
        Future<Void> f1 = executorService.submit
                (
                        new Callable<Void>()
                        {
                            @Override
                            public Void call() throws Exception
                            {
                                startLatch.await();
                                for (int i = 0; i < 1000; i++) {
                                    testSet.increment(1);
                                }
                                return null;
                            }
                        }
                );

        Future<Void> f2 = executorService.submit
                (
                        new Callable<Void>()
                        {
                            @Override
                            public Void call() throws Exception
                            {
                                startLatch.await();
                                for (int i = 0; i < 1000; i++) {
                                    testSet.increment(1);
                                }
                                return null;
                            }
                        }
                );

        startLatch.countDown();
        f1.get();
        f2.get();

        Assert.assertEquals(2000, testSet.getCount(1));
    }

    @Test
    public void testSimultaneousPutAndRemove() throws Exception {
        ExecutorService executorService = Executors.newCachedThreadPool();

        final CountDownLatch startLatch = new CountDownLatch(1);
        testSet.increment(1);
        Future<Void> f1 = executorService.submit
                (
                        new Callable<Void>()
                        {
                            @Override
                            public Void call() throws Exception
                            {
                                startLatch.await();
                                for (int i = 0; i < 1000; i++) {
                                    testSet.increment(1);
                                }
                                return null;
                            }
                        }
                );

        Future<Void> f2 = executorService.submit
                (
                        new Callable<Void>()
                        {
                            @Override
                            public Void call() throws Exception
                            {
                                startLatch.await();
                                for (int i = 0; i < 1000; i++) {
                                    testSet.decrement(1);
                                }
                                return null;
                            }
                        }
                );

        startLatch.countDown();
        f1.get();
        f2.get();

        // Data should be consistent now
        Assert.assertEquals(1, testSet.getCount(1));
    }

    // We are not interested in seeing if the data is consistent. We only want to know if there is no concurrent
    // modification exception thrown
    @Test
    public void testSimultaneousPutAndContains() throws Exception {
        ExecutorService executorService = Executors.newCachedThreadPool();

        final CountDownLatch startLatch = new CountDownLatch(1);

        Future<Void> f1 = executorService.submit
                (
                        new Callable<Void>()
                        {
                            @Override
                            public Void call() throws Exception
                            {
                                startLatch.await();
                                for (int i = 0; i < 1000; i++) {
                                    testSet.increment(1);
                                }
                                return null;
                            }
                        }
                );

        Future<Void> f2 = executorService.submit
                (
                        new Callable<Void>()
                        {
                            @Override
                            public Void call() throws Exception
                            {
                                startLatch.await();
                                for (int i = 0; i < 1000; i++) {
                                    testSet.contains(1);
                                }
                                return null;
                            }
                        }
                );

        startLatch.countDown();
        f1.get();
        f2.get();

        // Now the data should be consistent. Let's check
        Assert.assertTrue(testSet.contains(1));
    }

    @Test
    public void testContains() throws Exception {
        ExecutorService executorService = Executors.newCachedThreadPool();

        final CountDownLatch startLatch = new CountDownLatch(1);

        testSet.increment(1);

        Future<Void> f1 = executorService.submit
                (
                        new Callable<Void>()
                        {
                            @Override
                            public Void call() throws Exception
                            {
                                testSet.decrement(1);
                                startLatch.countDown();
                                return null;
                            }
                        }
                );

        Future<Void> f2 = executorService.submit
                (
                        new Callable<Void>()
                        {
                            @Override
                            public Void call() throws Exception
                            {
                                startLatch.await();
                                Assert.assertTrue(!testSet.contains(1));
                                return null;
                            }
                        }
                );

        f1.get();
        f2.get();
    }
}