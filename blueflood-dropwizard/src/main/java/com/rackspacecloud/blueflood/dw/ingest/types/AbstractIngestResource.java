package com.rackspacecloud.blueflood.dw.ingest.types;

import com.rackspacecloud.blueflood.dw.ingest.IngestConfiguration;
import com.rackspacecloud.blueflood.dw.ingest.ShardUpdates;
import com.rackspacecloud.blueflood.io.IMetricsWriter;
import com.rackspacecloud.blueflood.service.ScheduleContext;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.Util;

import java.io.IOException;
import java.util.Collection;

public abstract class AbstractIngestResource /*implements IMetricsWriter*/ {
    
    private final IMetricsWriter writer;
    private final ScheduleContext context;
    private final IngestConfiguration configuration;
    
    public AbstractIngestResource(IngestConfiguration configuration, ScheduleContext context, IMetricsWriter writer) {
        this.writer = writer;
        this.configuration = configuration;
        this.context = context;
    }
    
    protected void maybeForceCollectionTimes(long when, Collection<? extends ICollectionTime> metrics) {
        if (!configuration.getForceNewCollectionTime())
            return;
        
        for (ICollectionTime metric : metrics) {
            metric.setCollectionTime(when);
        }
    }
    
    protected void maybeForceCollectionTimes(long when, ICollectionTime metric) {
        if (!configuration.getForceNewCollectionTime())
            return;
        
        metric.setCollectionTime(when);
    }
    
    protected void updateContext(Collection<? extends IMetric> metrics) {
        ShardUpdates updates = new ShardUpdates();
        for (IMetric m : metrics) {
            updates.update(m.getCollectionTime(), Util.computeShard(m.getLocator().toString()));
        }
        updates.flush(context);
    }
    
    // overwrite these!
    
    public void preProcess(Collection<? extends IMetric> metrics) {}
    public void postProcess(Collection<? extends IMetric> metrics) {}
    
    // technically implements IMetricsWriter methods. simple delegation.

    public void insertFullMetrics(Collection<Metric> metrics) throws IOException {
        writer.insertFullMetrics(metrics);
    }

    public void insertPreaggreatedMetrics(Collection<IMetric> metrics) throws IOException {
        writer.insertPreaggreatedMetrics(metrics);
    }
}
