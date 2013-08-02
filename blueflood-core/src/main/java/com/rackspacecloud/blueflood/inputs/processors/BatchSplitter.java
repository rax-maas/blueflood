package com.rackspacecloud.blueflood.inputs.processors;

import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.MetricsCollection;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

public class BatchSplitter extends AsyncFunctionWithThreadPool<MetricsCollection, List<List<Metric>>> {
    
    private int numPartitions;
    
    public BatchSplitter(ThreadPoolExecutor threadPool, int numPartitions) {
        super(threadPool);
        this.numPartitions = numPartitions;
    }
    
    public ListenableFuture<List<List<Metric>>> apply(final MetricsCollection input) throws Exception {
        return getThreadPool().submit(new Callable<List<List<Metric>>>() {
            public List<List<Metric>> call() throws Exception {
                return MetricsCollection.getMetricsAsBatches(input, numPartitions);
            }
        });    
    }
    
    public void setNumPartitions(int i) {
        this.numPartitions = i;
    }
}
