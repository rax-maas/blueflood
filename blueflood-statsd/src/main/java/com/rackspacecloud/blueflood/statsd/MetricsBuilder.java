package com.rackspacecloud.blueflood.statsd;

import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MetricsBuilder extends AsyncFunctionWithThreadPool<Collection<Stat>, Collection<Metric>> {
    private static Logger log = LoggerFactory.getLogger(MetricsBuilder.class);
    
    private TimeValue ttl = new TimeValue(3, TimeUnit.DAYS);
    
    public MetricsBuilder(ThreadPoolExecutor executor) {
        super(executor);
    }
    
    public MetricsBuilder withTTL(TimeValue ttl) {
        this.ttl = ttl;
        return this;
    }

    @Override
    public ListenableFuture<Collection<Metric>> apply(final Collection<Stat> input) throws Exception {
        return getThreadPool().submit(new Callable<Collection<Metric>>() {
            @Override
            public Collection<Metric> call() throws Exception {
                Collection<Metric> metrics = new ArrayList<Metric>(input.size());
                for (Stat stat : input) {
                    Metric metric = Stat.asMetric(stat);
                    if (metric != null) {
                        metric.setTtl(ttl);
                        metrics.add(metric);
                    } else {
                        log.debug("Null metric from {}", stat.toString());
                    }
                }
                return metrics;
            }
        });
    }

}
