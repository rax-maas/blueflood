package com.rackspacecloud.blueflood.dw.ingest;

import com.codahale.metrics.annotation.Timed;
import com.rackspacecloud.blueflood.io.IMetricsWriter;
import com.rackspacecloud.blueflood.service.ScheduleContext;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.TimeValue;

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Path("/v1.0/{tenantId}/ingest/basic")
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
    public IngestResponseRepresentation storeMetrics(final @PathParam("tenantId") String tenantId, final @QueryParam("commitReceipt") String commitReceipt, List<BasicMetric> metrics) {
        try {
            writer.insertFullMetrics(remarshall(metrics, tenantId));
        } catch (IOException ex) {
            throw new WebApplicationException(ex, Response.Status.INTERNAL_SERVER_ERROR);
        }
        
        // todo: block until the commitReceipt proves durable.
        
        IngestResponseRepresentation response = new IngestResponseRepresentation("OK accepted");
        return response;
    }
    
    private static Collection<Metric> remarshall(Collection<BasicMetric> basicMetrics, String tenantId) {
        List<Metric> metrics = new ArrayList<Metric>(basicMetrics.size());
        for (BasicMetric bm : basicMetrics) {
            Locator locator = Locator.createLocatorFromPathComponents(tenantId, bm.getMetricName().split("\\.", -1));
            Metric m = new Metric(locator, bm.getMetricValue(), bm.getCollectionTime(), new TimeValue(bm.getTtlInSeconds(), TimeUnit.SECONDS), bm.getUnit());
            metrics.add(m);
        }
        return metrics;
    }
}
