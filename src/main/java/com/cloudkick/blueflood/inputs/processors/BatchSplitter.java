package com.cloudkick.blueflood.inputs.processors;

import com.cloudkick.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.cloudkick.blueflood.types.Metric;
import com.cloudkick.blueflood.types.MetricsCollection;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.util.List;
import java.util.concurrent.Callable;

public class BatchSplitter extends AsyncFunctionWithThreadPool<MetricsCollection, List<List<Metric>>> {
    
    private final int numPartitions;
    
    public BatchSplitter(ListeningExecutorService threadPool, int numPartitions) {
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
}
