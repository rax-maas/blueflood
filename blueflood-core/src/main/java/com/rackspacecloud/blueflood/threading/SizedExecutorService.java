package com.rackspacecloud.blueflood.threading;

import java.util.concurrent.ExecutorService;

public interface SizedExecutorService extends ExecutorService {
    int getActiveCount();
    int getPoolSize();
}
