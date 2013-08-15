package com.rackspacecloud.blueflood.inputs.processors;

import com.rackspacecloud.blueflood.cache.TtlCache;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.MetricsCollection;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

public class TtlAffixer extends AsyncFunctionWithThreadPool<MetricsCollection, MetricsCollection> {
        
    private final TtlCache cache;
    
    public TtlAffixer(ThreadPoolExecutor threadPool, TtlCache cache) {
        super(threadPool);
        this.cache = cache;
    }
    
    public ListenableFuture<MetricsCollection> apply(final MetricsCollection metrics) {
        return getThreadPool().submit(new Callable<MetricsCollection>() {
            public MetricsCollection call() throws Exception {
                for (Metric metric : metrics.getMetrics()) {
                    metric.setTtlInSeconds((int)cache.getTtl(metric.getLocator().getTenantId(), Granularity.FULL).toSeconds());
                }
                return metrics;
            }
        });
    }
}
