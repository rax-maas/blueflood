package com.rackspacecloud.blueflood.dw.ingest;

import com.codahale.metrics.annotation.Timed;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.dw.ingest.types.BasicMetric;
import com.rackspacecloud.blueflood.dw.ingest.types.Bundle;
import com.rackspacecloud.blueflood.dw.ingest.types.Marshal;
import com.rackspacecloud.blueflood.io.IMetricsWriter;
import com.rackspacecloud.blueflood.service.ScheduleContext;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Metric;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

@Path("/v2.0/{tenantId}/ingest")
@Produces(MediaType.APPLICATION_JSON)
public class BasicIngestResource extends AbstractIngestResource {
    
    public BasicIngestResource(IngestConfiguration configuration, ScheduleContext context, IMetricsWriter writer, MetadataCache cache) {
        super(configuration, context, writer, cache);
    }
    
    @POST
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("basic")
    public IngestResponseRepresentation saveBasicMetrics(final @PathParam("tenantId") String tenantId, final @QueryParam("commitReceipt") String commitReceipt, List<BasicMetric> metrics) {
        try {
            maybeForceCollectionTimes(System.currentTimeMillis(), metrics);
            Collection<Metric> newMetrics = Marshal.remarshal(metrics, tenantId);
            processTypeAndUnit(newMetrics);
            preProcess(newMetrics);
            insertFullMetrics(newMetrics);
            updateContext(newMetrics);
            postProcess(newMetrics);
        } catch (IOException ex) {
            throw new WebApplicationException(ex, Response.Status.INTERNAL_SERVER_ERROR);
        }
        
        // todo: block until the commitReceipt proves durable.
        
        return new IngestResponseRepresentation("OK accepted");
    }
    
    @POST
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("aggregated")
    public IngestResponseRepresentation savePreagMetrics(final @PathParam("tenantId") String tenantId, final @QueryParam("commitReceipt") String commitReceipt, Bundle bundle) {
        try {
            maybeForceCollectionTimes(System.currentTimeMillis(), bundle);
            Collection<IMetric> newMetrics = Marshal.remarshal(bundle, tenantId);
            preProcess(newMetrics);
            insertPreaggreatedMetrics(newMetrics);
            updateContext(newMetrics);
            postProcess(newMetrics);
        } catch (IOException ex) {
            throw new WebApplicationException(ex, Response.Status.INTERNAL_SERVER_ERROR);
        }
        
        // todo: block for commitReceipt
        
        return new IngestResponseRepresentation("OK accepted");
    }
    
}
