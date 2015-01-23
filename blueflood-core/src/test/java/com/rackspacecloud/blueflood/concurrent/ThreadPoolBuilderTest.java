package com.rackspacecloud.blueflood.concurrent;

import junit.framework.TestCase;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;

public class ThreadPoolBuilderTest extends TestCase {

    public void testWithSynchronousQueue() throws Exception {
        ThreadPoolExecutor tpe = new ThreadPoolBuilder()
                .withSynchronousQueue()
                .build();
        assertEquals(SynchronousQueue.class, tpe.getQueue().getClass());
    }

    public void testWithUnboundedQueue() throws Exception {
        ThreadPoolExecutor tpe = new ThreadPoolBuilder()
                .withUnboundedQueue()
                .build();
        assertEquals(LinkedBlockingQueue.class, tpe.getQueue().getClass());
    }

    public void testWithBoundedQueue() throws Exception {
        int queueSize = 70;
        ThreadPoolExecutor tpe = new ThreadPoolBuilder()
                .withBoundedQueue(queueSize)
                .build();
        assertEquals(ArrayBlockingQueue.class, tpe.getQueue().getClass());
        assertEquals(queueSize, tpe.getQueue().remainingCapacity());
    }
}