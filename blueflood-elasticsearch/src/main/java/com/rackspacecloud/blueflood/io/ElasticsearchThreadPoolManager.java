package com.rackspacecloud.blueflood.io;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;

import java.util.concurrent.ThreadPoolExecutor;

public class ElasticsearchThreadPoolManager {
    public final ListeningExecutorService listeningExecutorService;
    private final ThreadPoolExecutor threadPoolExecutor;

    public int remainingCapacityOfTheQueue(){
        return this.threadPoolExecutor.getQueue().remainingCapacity();
    }

    public ElasticsearchThreadPoolManager(ThreadPoolExecutor threadPoolExecutor){
        this.threadPoolExecutor = threadPoolExecutor;
        this.listeningExecutorService = MoreExecutors.listeningDecorator(this.threadPoolExecutor);
    }

    public ElasticsearchThreadPoolManager(String threadPoolName, int corePoolSize, int maxPoolSize){
        this(new ThreadPoolBuilder()
                .withName(threadPoolName)
                .withCorePoolSize(corePoolSize)
                .withMaxPoolSize(maxPoolSize)
                .withUnboundedQueue()
                .build());
    }
}