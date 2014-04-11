package com.rackspacecloud.blueflood.dw.ingest;

import com.codahale.metrics.annotation.Timed;
import com.rackspacecloud.blueflood.dw.ingest.types.BasicMetric;
import com.rackspacecloud.blueflood.dw.ingest.types.Bundle;
import com.rackspacecloud.blueflood.dw.ingest.types.Counter;
import com.rackspacecloud.blueflood.dw.ingest.types.Gauge;
import com.rackspacecloud.blueflood.dw.ingest.types.Marshal;
import com.rackspacecloud.blueflood.dw.ingest.types.Set;
import com.rackspacecloud.blueflood.dw.ingest.types.Timer;
import com.rackspacecloud.blueflood.io.IMetricsWriter;
import com.rackspacecloud.blueflood.service.ScheduleContext;

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
import java.util.List;

@Path("/v1.0/{tenantId}/ingest")
@Produces(MediaType.APPLICATION_JSON)
public class BasicIngestResource {
    
    private IMetricsWriter writer;
    private ScheduleContext context;
    
    
    public BasicIngestResource(ScheduleContext context, IMetricsWriter writer) {
        this.context = context;
        this.writer = writer;
    }
    
    @POST
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("basic")
    public IngestResponseRepresentation saveBasicMetrics(final @PathParam("tenantId") String tenantId, final @QueryParam("commitReceipt") String commitReceipt, List<BasicMetric> metrics) {
        try {
            writer.insertFullMetrics(Marshal.remarshal(metrics, tenantId));
        } catch (IOException ex) {
            throw new WebApplicationException(ex, Response.Status.INTERNAL_SERVER_ERROR);
        }
        
        // todo: block until the commitReceipt proves durable.
        
        return new IngestResponseRepresentation("OK accepted");
    }
    
    @POST
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("basic/scoped")
    public IngestResponseRepresentation saveBasicMultiTenantMetrics(final @QueryParam("commitReceipt") String commitReceipt, List<BasicMetric> metrics) {
        
        // if any metrics are missing a tenant, fail.
        for (BasicMetric bm : metrics) {
            if (bm.getTenant() == null || bm.getTenant().trim().length() == 0) {
                throw new WebApplicationException(Response
                        .status(Response.Status.BAD_REQUEST)
                        .header("X-Reason", "One or more metrics does not specify tenant")
                        .build());
            }
        }
        
        try {
            writer.insertFullMetrics(Marshal.remarshal(metrics, null));
        } catch (IOException ex) {
            throw new WebApplicationException(ex, Response.Status.INTERNAL_SERVER_ERROR);
        }
        
        // todo: block until the commitReceipt is verified durable.
        
        return new IngestResponseRepresentation("OK accepted");
    }
    
    @POST
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("aggregated")
    public IngestResponseRepresentation savePreagMetrics(final @PathParam("tenantId") String tenantId, final @QueryParam("commitReceipt") String commitReceipt, Bundle bundle) {
        try {
            writer.insertPreaggreatedMetrics(Marshal.remarshal(bundle, tenantId));
        } catch (IOException ex) {
            throw new WebApplicationException(ex, Response.Status.INTERNAL_SERVER_ERROR);
        }
        
        // todo: block for commitReceipt
        
        return new IngestResponseRepresentation("OK accepted");
    }
    
    @POST
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("aggregated/scoped")
    public IngestResponseRepresentation savePreagMultiTenantMetrics(final @QueryParam("commitReceipt") String commitReceipt, Bundle bundle) {
        
        // if any metric is missing a tenant, fail.
        for (Gauge g : bundle.getGauges()) {
            if (g.getTenant() == null || g.getTenant().trim().length() == 0) {
                throw new WebApplicationException(Response
                        .status(Response.Status.BAD_REQUEST)
                        .header("X-Reason", "One ore more metrics does not specify tenant")
                        .build());
            }
        }
        
        for (Set s : bundle.getSets()) {
            if (s.getTenant() == null || s.getTenant().trim().length() == 0) {
                throw new WebApplicationException(Response
                        .status(Response.Status.BAD_REQUEST)
                        .header("X-Reason", "One ore more metrics does not specify tenant")
                        .build());
            }
        }
        
        for (Counter c : bundle.getCounters()) {
            if (c.getTenant() == null || c.getTenant().trim().length() == 0) {
                throw new WebApplicationException(Response
                        .status(Response.Status.BAD_REQUEST)
                        .header("X-Reason", "One ore more metrics does not specify tenant")
                        .build());
            }
        }
        
        for (Timer t : bundle.getTimers()) {
            if (t.getTenant() == null || t.getTenant().trim().length() == 0) {
                throw new WebApplicationException(Response
                        .status(Response.Status.BAD_REQUEST)
                        .header("X-Reason", "One ore more metrics does not specify tenant")
                        .build());
            }
        }
        
        try {
            writer.insertPreaggreatedMetrics(Marshal.remarshal(bundle, null));
        } catch (IOException ex) {
            throw new WebApplicationException(ex, Response.Status.INTERNAL_SERVER_ERROR);
        }
        
        // todo: block for commit receipt.
        
        return new IngestResponseRepresentation("OK accepted");
    }
}
