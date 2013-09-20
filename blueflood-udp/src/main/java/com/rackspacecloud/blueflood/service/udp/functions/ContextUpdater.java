package com.rackspacecloud.blueflood.service.udp.functions;

import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.rackspacecloud.blueflood.concurrent.NoOpFuture;
import com.rackspacecloud.blueflood.service.ScheduleContext;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.Util;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * Simple demonstratin of a AsyncFunctionWithThreadPool that does not use its threadpool.  It does things on
 * whichever thread AsyncChain.apply() was called on.
 */
public class ContextUpdater extends AsyncFunctionWithThreadPool<Metric, Metric> {
    
    private final ScheduleContext context;
    
    public ContextUpdater(ThreadPoolExecutor threadPool, ScheduleContext context) {
        super(threadPool);
        this.context = context;
    }

    @Override
    public ListenableFuture<Metric> apply(Metric input) throws Exception {
        // this is a quick operation, so do not use the threadpool.  Just do the work and return a NoOpFuture.
        context.update(input.getCollectionTime(), Util.computeShard(input.getLocator().toString()));
        return new NoOpFuture<Metric>(input);
    }
}
