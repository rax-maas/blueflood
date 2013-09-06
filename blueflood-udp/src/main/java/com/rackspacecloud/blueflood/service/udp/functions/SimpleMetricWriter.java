package com.rackspacecloud.blueflood.service.udp.functions;

import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.types.Metric;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

/** Writes a single metric to the database. */
public class SimpleMetricWriter extends AsyncFunctionWithThreadPool<Metric, Metric> {
    
    public SimpleMetricWriter(ThreadPoolExecutor threadPool) {
        super(threadPool);
    }

    @Override
    public ListenableFuture<Metric> apply(final Metric input) throws Exception {
        return getThreadPool().submit(new Callable<Metric>() {
            public Metric call() throws Exception {
                List<Metric> metrics = asList(input);
                AstyanaxWriter.getInstance().insertFull(metrics);
                getLogger().info("wrote " + input.getLocator().toString());
                return input;
            }
        });
    }
    
    private static final List<Metric> asList(Metric m) {
        List list = new ArrayList<Metric>(1);
        list.add(m);
        return list;
    }
}
