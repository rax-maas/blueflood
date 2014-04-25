package com.rackspacecloud.blueflood.dw.ingest;

import com.codahale.metrics.Meter;
import com.codahale.metrics.annotation.Timed;
import com.rackspacecloud.blueflood.cache.MetadataCache;
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
import com.rackspacecloud.blueflood.utils.Metrics;

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

@Path("/v2.0/ingest")
@Produces(MediaType.APPLICATION_JSON)
public class MultiTenantIngestResource extends AbstractIngestResource {
    
    private final Meter err4xxMeter = Metrics.meter(MultiTenantIngestResource.class, "4xx Errors");
    private final Meter err5xxMeter = Metrics.meter(MultiTenantIngestResource.class, "5xx Errors");

    public MultiTenantIngestResource(IngestConfiguration configuration, ScheduleContext context, IMetricsWriter writer, MetadataCache cache) {
        super(configuration, context, writer, cache);
    }

    @POST
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("basic")
    public void saveBasicMultiTenantMetrics(final @PathParam("tenantId") String tenantId, final @QueryParam("commitReceipt") String commitReceipt, List<BasicMetric> metrics) {
        
        // if any metrics are missing a tenant, fail.
        for (BasicMetric bm : metrics) {
            if (bm.getTenant() == null || bm.getTenant().trim().length() == 0) {
                err4xxMeter.mark();
                throw new WebApplicationException(Response
                        .status(Response.Status.BAD_REQUEST)
                        .entity(new SimpleResponse("One or more metrics does not specify tenant"))
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
            err5xxMeter.mark();
            throw new WebApplicationException(ex, Response.Status.INTERNAL_SERVER_ERROR);
        }
        
        // todo: block until the commitReceipt is verified durable.
    }
    
    @POST
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("aggregated")
    public void savePreagMultiTenantMetrics(final @PathParam("tenantId") String tenantId, final @QueryParam("commitReceipt") String commitReceipt, Bundle bundle) {
        
        // if any metric is missing a tenant, fail.
        for (Gauge g : bundle.getGauges()) {
            if (g.getTenant() == null || g.getTenant().trim().length() == 0) {
                err4xxMeter.mark();
                throw new WebApplicationException(Response
                        .status(Response.Status.BAD_REQUEST)
                        .entity("One ore more metrics does not specify tenant")
                        .build());
            }
        }
        
        for (Set s : bundle.getSets()) {
            if (s.getTenant() == null || s.getTenant().trim().length() == 0) {
                err4xxMeter.mark();
                throw new WebApplicationException(Response
                        .status(Response.Status.BAD_REQUEST)
                        .entity("One ore more metrics does not specify tenant")
                        .build());
            }
        }
        
        for (Counter c : bundle.getCounters()) {
            if (c.getTenant() == null || c.getTenant().trim().length() == 0) {
                err4xxMeter.mark();
                throw new WebApplicationException(Response
                        .status(Response.Status.BAD_REQUEST)
                        .entity("One ore more metrics does not specify tenant")
                        .build());
            }
        }
        
        for (Timer t : bundle.getTimers()) {
            if (t.getTenant() == null || t.getTenant().trim().length() == 0) {
                err4xxMeter.mark();
                throw new WebApplicationException(Response
                        .status(Response.Status.BAD_REQUEST)
                        .entity("One ore more metrics does not specify tenant")
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
            err5xxMeter.mark();
            throw new WebApplicationException(ex, Response.Status.INTERNAL_SERVER_ERROR);
        }
        
        // todo: block for commit receipt.
        
    }
}
