package com.cloudkick.blueflood.inputs.processors;

import com.cloudkick.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.cloudkick.blueflood.exceptions.IncomingMetricException;
import com.cloudkick.blueflood.concurrent.NoOpFuture;
import com.cloudkick.blueflood.service.IncomingMetricMetadataAnalyzer;
import com.cloudkick.blueflood.types.MetricsCollection;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

public class TypeAndUnitProcessor extends AsyncFunctionWithThreadPool<MetricsCollection, MetricsCollection> {
        
    final IncomingMetricMetadataAnalyzer metricMetadataAnalyzer;
    
    public TypeAndUnitProcessor(ThreadPoolExecutor threadPool, IncomingMetricMetadataAnalyzer metricMetadataAnalyzer) {
        super(threadPool);
        this.metricMetadataAnalyzer = metricMetadataAnalyzer;
    }
    
    public ListenableFuture<MetricsCollection> apply(final MetricsCollection input) throws Exception {
        getThreadPool().submit(new Callable<MetricsCollection>() {
            public MetricsCollection call() throws Exception {
                Collection<IncomingMetricException> problems = metricMetadataAnalyzer.scanMetrics(input.getMetrics());
                for (IncomingMetricException problem : problems)
                    // TODO: this is where a system annotation should be raised.
                    getLogger().warn(problem.getMessage());
                return input;
            }
        });
        
        // this one is asynchronous. so we let it do its job offline in the threadpool, but return a future that is
        // immediately done.
        return new NoOpFuture<MetricsCollection>(input);
    }
}