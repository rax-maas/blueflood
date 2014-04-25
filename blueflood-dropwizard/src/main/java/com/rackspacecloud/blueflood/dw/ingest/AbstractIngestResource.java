package com.rackspacecloud.blueflood.dw.ingest;

import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.dw.ingest.types.ICollectionTime;
import com.rackspacecloud.blueflood.io.IMetricsWriter;
import com.rackspacecloud.blueflood.service.ScheduleContext;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.MetricMetadata;
import com.rackspacecloud.blueflood.utils.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

public abstract class AbstractIngestResource /*implements IMetricsWriter*/ {
    protected static final Logger log = LoggerFactory.getLogger(AbstractIngestResource.class);
    
    private static final String ROLLUP_TYPE_CACHE_KEY = MetricMetadata.ROLLUP_TYPE.name().toLowerCase();
    private static final String DATA_TYPE_CACHE_KEY = MetricMetadata.TYPE.name().toLowerCase();
    private static final String UNIT_CACHE_KEY = MetricMetadata.UNIT.name().toLowerCase();
    
    private final IMetricsWriter writer;
    private final ScheduleContext context;
    private final IngestConfiguration configuration;
    private final MetadataCache cache;
    
    public AbstractIngestResource(IngestConfiguration configuration, ScheduleContext context, IMetricsWriter writer, MetadataCache cache) {
        this.writer = writer;
        this.configuration = configuration;
        this.context = context;
        this.cache = cache;
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
    
    // this only happens on basic full resolution data.
    protected final void processTypeAndUnit(Collection<? extends IMetric> metrics) {
        for (IMetric m : metrics) {
            try {
                String existingType = cache.get(m.getLocator(), DATA_TYPE_CACHE_KEY);
                String existingUnit = cache.get(m.getLocator(), UNIT_CACHE_KEY);
                
                if (m instanceof Metric) {
                    Metric mm = (Metric)m;
                    cache.put(m.getLocator(), DATA_TYPE_CACHE_KEY, mm.getDataType().toString());
                    
                    if (mm.getUnit() != null) {
                        cache.put(m.getLocator(), UNIT_CACHE_KEY, mm.getUnit());
                    }
                    
                    // log mismatches.
                    if (existingType != null && !existingType.equals(mm.getDataType().toString())) {
                        log.warn("Types changed for {}. From {} to {}", new Object[] {m.getLocator().toString(), existingType, mm.getDataType().toString()});
                    }
                    if (existingUnit != null && !existingUnit.equals(mm.getUnit())) {
                        log.warn("Units changed for {}. From {} to {}", new Object[] {m.getLocator().toString(), existingUnit, mm.getUnit()});
                    }
                }
            
                cache.put(m.getLocator(), ROLLUP_TYPE_CACHE_KEY, m.getRollupType().toString());
            } catch (Throwable th) {
                // there's nothing we can do about a problem, besides log it. errors here do not necessarily indicate
                // that a metric will not be ingested.
                log.warn(th.getMessage(), th);
            }
        }
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
