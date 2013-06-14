package com.cloudkick.blueflood.inputs.processors;

import com.cloudkick.blueflood.cache.TtlCache;
import com.cloudkick.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.cloudkick.blueflood.rollup.Granularity;
import com.cloudkick.blueflood.types.Metric;
import com.cloudkick.blueflood.types.MetricsCollection;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.util.concurrent.Callable;

public class TtlAffixer extends AsyncFunctionWithThreadPool<MetricsCollection, MetricsCollection> {
        
    private final TtlCache cache;
    
    public TtlAffixer(ListeningExecutorService threadPool, TtlCache cache) {
        super(threadPool);
        this.cache = cache;
    }
    
    public ListenableFuture<MetricsCollection> apply(final MetricsCollection metrics) {
        return getThreadPool().submit(new Callable<MetricsCollection>() {
            public MetricsCollection call() throws Exception {
                for (Metric metric : metrics.getMetrics()) {
                    metric.setTtlInSeconds((int)cache.getTtl(metric.getLocator().getAccountId(), Granularity.FULL).toSeconds());
                }
                return metrics;
            }
        });
    }
}
