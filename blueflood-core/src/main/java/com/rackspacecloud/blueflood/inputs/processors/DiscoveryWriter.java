package com.rackspacecloud.blueflood.inputs.processors;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.rackspacecloud.blueflood.io.RackIO;
import com.rackspacecloud.blueflood.types.Metric;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

// CM_SPECIFIC
public class DiscoveryWriter extends AsyncFunctionWithThreadPool<List<List<Metric>>, List<Boolean>> {
    
    private final RackIO rackIO;
    
    public DiscoveryWriter(ThreadPoolExecutor threadPool, RackIO rackIO) {
        super(threadPool);
        this.rackIO = rackIO;
    }

    @Override
    public ListenableFuture<List<Boolean>> apply(List<List<Metric>> input) throws Exception {
        
        final List<ListenableFuture<Boolean>> resultFutures = new ArrayList<ListenableFuture<Boolean>>();
        
        for (List<Metric> metrics : input) {
            final List<Metric> batch = metrics;
            ListenableFuture<Boolean> futureBatchResult = getThreadPool().submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    try {
                        rackIO.insertDiscovery(batch);
                        return true;
                    } catch (Exception ex) {
                        getLogger().error(ex.getMessage(), ex);
                        return false;
                    }
                }
            });
            
            resultFutures.add(futureBatchResult);
        }
        
        return Futures.allAsList(resultFutures);
    }
}
