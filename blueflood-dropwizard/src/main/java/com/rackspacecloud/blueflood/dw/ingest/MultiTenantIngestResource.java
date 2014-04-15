package com.rackspacecloud.blueflood.dw.ingest;

import com.codahale.metrics.annotation.Timed;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.dw.ingest.AbstractIngestResource;
import com.rackspacecloud.blueflood.dw.ingest.IngestConfiguration;
import com.rackspacecloud.blueflood.dw.ingest.IngestResponseRepresentation;
import com.rackspacecloud.blueflood.dw.ingest.types.BasicMetric;
import com.rackspacecloud.blueflood.dw.ingest.types.Bundle;
import com.rackspacecloud.blueflood.dw.ingest.types.Counter;
import com.rackspacecloud.blueflood.dw.ingest.types.Gauge;
import com.rackspacecloud.blueflood.dw.ingest.types.Marshal;
import com.rackspacecloud.blueflood.dw.ingest.types.Set;
import com.rackspacecloud.blueflood.dw.ingest.types.Timer;
import com.rackspacecloud.blueflood.io.IMetricsWriter;
import com.rackspacecloud.blueflood.service.ScheduleContext;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Metric;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

@Path("/2.0/ingest")
public class MultiTenantIngestResource extends AbstractIngestResource {

    public MultiTenantIngestResource(IngestConfiguration configuration, ScheduleContext context, IMetricsWriter writer, MetadataCache cache) {
        super(configuration, context, writer, cache);
    }

    @POST
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("basic")
    public IngestResponseRepresentation saveBasicMultiTenantMetrics(final @PathParam("tenantId") String tenantId, final @QueryParam("commitReceipt") String commitReceipt, List<BasicMetric> metrics) {
        
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
            maybeForceCollectionTimes(System.currentTimeMillis(), metrics);
            Collection<Metric> newMetrics = Marshal.remarshal(metrics, null);
            processTypeAndUnit(newMetrics);
            preProcess(newMetrics);
            insertFullMetrics(newMetrics);
            updateContext(newMetrics);
            postProcess(newMetrics);
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
    public IngestResponseRepresentation savePreagMultiTenantMetrics(final @PathParam("tenantId") String tenantId, final @QueryParam("commitReceipt") String commitReceipt, Bundle bundle) {
        
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
            maybeForceCollectionTimes(System.currentTimeMillis(), bundle);
            Collection<IMetric> newMetrics = Marshal.remarshal(bundle, null);
            preProcess(newMetrics);
            insertPreaggreatedMetrics(newMetrics);
            updateContext(newMetrics);
            postProcess(newMetrics);
        } catch (IOException ex) {
            throw new WebApplicationException(ex, Response.Status.INTERNAL_SERVER_ERROR);
        }
        
        // todo: block for commit receipt.
        
        return new IngestResponseRepresentation("OK accepted");
    }
}
