package com.rackspacecloud.blueflood.dw.ingest;

import com.codahale.metrics.annotation.Timed;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("/v1.0/{tenantId}/ingest/basic")
@Produces(MediaType.APPLICATION_JSON)
public class BasicIngestResource {
    
    public BasicIngestResource() {
    }
    
    @POST
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public IngestResponseRepresentation storeMetrics(final @PathParam("tenantId") String tenantId, final @QueryParam("commitReceipt") String commitReceipt, List<BasicMetric> metrics) {
        IngestResponseRepresentation response = new IngestResponseRepresentation();
        response.setMessage("OK good");
        return response;
    }
}
