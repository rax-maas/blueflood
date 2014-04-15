package com.rackspacecloud.blueflood.dw.ingest;

import com.codahale.metrics.annotation.Timed;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.dw.ingest.types.BasicMetric;
import com.rackspacecloud.blueflood.io.IMetricsWriter;
import com.rackspacecloud.blueflood.service.ScheduleContext;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.MetricMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.Collection;
import java.util.List;

@Deprecated
@Path("/v1.0/{tenantId}/experimental/metrics")
@Produces(MediaType.APPLICATION_JSON)
public class LegacyBasicIngestResource extends BasicIngestResource {
    private static final Logger log = LoggerFactory.getLogger(LegacyBasicIngestResource.class);
    private static final String ROLLUP_TYPE_CACHE_KEY = MetricMetadata.ROLLUP_TYPE.name().toLowerCase();
    private static final String DATA_TYPE_CACHE_KEY = MetricMetadata.TYPE.name().toLowerCase();
    private static final String UNIT_CACHE_KEY = MetricMetadata.UNIT.name().toLowerCase();
    
    private final MetadataCache metadataCache;
    
    public LegacyBasicIngestResource(IngestConfiguration configuration, ScheduleContext context, IMetricsWriter writer) {
        super(configuration, context, writer);
        metadataCache = MetadataCache.getInstance();
    }
    
    @POST
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public IngestResponseRepresentation ingest(final @PathParam("tenantId") String tenantId, final @QueryParam("commitReceipt") String commitReceipt, List<BasicMetric> metrics) {
        return super.saveBasicMetrics(tenantId, commitReceipt, metrics);
    }

    // this method covers the work that was previously handled by TypeAndUnitProcessor(IncomingMetricMetadataAnalyzer)
    // and RollupTypcCacher.
    // todo: consider doing this asynchronously. Concurrency could be up to the setting for MAX_SCRIBE_WRITE_THREADS.
    @Override
    public void preProcess(Collection<? extends IMetric> metrics) {
        super.preProcess(metrics); // noop, but what the hey.
        
        // for basic metrics (Metric instances), cache data type and units. for every metric, cache the rollup type.
        for (IMetric m : metrics) {
            try {
                if (m instanceof Metric) {
                    // check type
                    Metric mm = (Metric)m;
                    String existingType = metadataCache.get(m.getLocator(), DATA_TYPE_CACHE_KEY);
                    String existingUnit = metadataCache.get(m.getLocator(), UNIT_CACHE_KEY);
                    metadataCache.put(m.getLocator(), DATA_TYPE_CACHE_KEY, mm.getDataType().toString());
                    
                    if (mm.getUnit() != null) {
                        metadataCache.put(m.getLocator(), UNIT_CACHE_KEY, mm.getUnit());
                    }
                    
                    // log mismatches.
                    if (existingType != null && !existingType.equals(mm.getDataType().toString())) {
                        log.warn("Types changed for {}. From {} to {}", new Object[] {m.getLocator().toString(), existingType, mm.getDataType().toString()});
                    }
                    if (existingUnit != null && !existingUnit.equals(mm.getUnit())) {
                        log.warn("Units changed for {}. From {} to {}", new Object[] {m.getLocator().toString(), existingUnit, mm.getUnit()});
                    }
                }
                
                metadataCache.put(m.getLocator(), ROLLUP_TYPE_CACHE_KEY, m.getRollupType().toString());
            } catch (Throwable th) {
                // there's nothing we can do about a problem, besides log it. errors here do not necessarily indicate
                // that a metric will not be ingested.
                log.warn(th.getMessage(), th);
            }
        }
        
    }
}
