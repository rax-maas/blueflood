package com.rackspacecloud.blueflood.threading;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

public interface SizedExecutorService extends ExecutorService {

    /**
     * Returns the approximate number of threads that are actively
     * executing tasks.
     *
     * @return the number of threads
     *
     * @see ThreadPoolExecutor#getActiveCount()
     */
    int getActiveCount();

    /**
     * Returns the current number of threads in the pool.
     *
     * @return the number of threads
     *
     * @see ThreadPoolExecutor#getPoolSize()
     */
    int getPoolSize();
}
