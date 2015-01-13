package com.rackspacecloud.blueflood.service.udp.functions;

import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.concurrent.FunctionWithThreadPool;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.types.Metric;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

/** Writes a single metric to the database. */
public class SimpleMetricWriter extends FunctionWithThreadPool<Collection<Metric>,  ListenableFuture<Collection<Metric>>> {
    
    public SimpleMetricWriter(ThreadPoolExecutor threadPool) {
        super(threadPool);
    }

    @Override
    public ListenableFuture<Collection<Metric>> apply(final Collection<Metric> input) throws Exception {
        return getThreadPool().submit(new Callable<Collection<Metric>>() {
            public Collection<Metric> call() throws Exception {
                AstyanaxWriter.getInstance().insertFull(asList(input));
                getLogger().info("wrote " + input.size() + " metrics");
                return input;
            }
        });
    }
    
    private static final List<Metric> asList(Collection<Metric> metrics) {
        if (metrics instanceof java.util.List)
            return (List<Metric>)metrics;
        else {
            List list = new ArrayList<Metric>(metrics);
            return list;
        }
    }
}
