package com.rackspacecloud.blueflood.statsd;

import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.MetricsCollection;
import com.rackspacecloud.blueflood.utils.TimeValue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MetricConverter extends AsyncFunctionWithThreadPool<Collection<Stat>, MetricsCollection> {
    
    private static final TimeValue DEFAULT_TTL = new TimeValue(48, TimeUnit.HOURS);
    
    public MetricConverter(ThreadPoolExecutor executor) {
        super(executor);
    }

    @Override
    public ListenableFuture<MetricsCollection> apply(final Collection<Stat> input) throws Exception {
        return getThreadPool().submit(new Callable<MetricsCollection>() {
            @Override
            public MetricsCollection call() throws Exception {
                List<Metric> metricList = new ArrayList<Metric>();
                for (Stat stat : input) {
                    // technicaly, the bad ones should have been thrown out already, but we do this incase this
                    // class gets reused.
                    if (stat.isValid()) {
                        metricList.add(new Metric(stat.getLocator(), stat.getValue(), stat.getTimestamp() * 1000, DEFAULT_TTL, null));
                    }
                }
                MetricsCollection metrics = new MetricsCollection();
                metrics.add(metricList);
                return metrics;
            }
        });
    }
}
