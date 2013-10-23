package com.rackspacecloud.blueflood.statsd;

import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.rackspacecloud.blueflood.service.ScheduleContext;
import com.rackspacecloud.blueflood.statsd.containers.TypedMetricsCollection;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.utils.Util;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

public class ContextUpdater extends AsyncFunctionWithThreadPool<TypedMetricsCollection, TypedMetricsCollection> {
    private final ScheduleContext context;
    
    public ContextUpdater(ScheduleContext context, ThreadPoolExecutor executor) {
        super(executor);
        this.context = context;
    }
    
    @Override
    public ListenableFuture<TypedMetricsCollection> apply(final TypedMetricsCollection input) throws Exception {
        return getThreadPool().submit(new Callable<TypedMetricsCollection>() {
            @Override
            public TypedMetricsCollection call() throws Exception {
                for (IMetric metric : input.getNormalMetrics())
                    context.update(metric.getCollectionTime(), Util.computeShard(metric.getLocator().toString()));
                for (IMetric metric : input.getPreaggregatedMetrics())
                    context.update(metric.getCollectionTime(), Util.computeShard(metric.getLocator().toString()));
                return input;
            }
        });
    }
}
