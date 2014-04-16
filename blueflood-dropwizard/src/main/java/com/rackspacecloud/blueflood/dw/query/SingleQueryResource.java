package com.rackspacecloud.blueflood.dw.query;

import com.codahale.metrics.annotation.Timed;
import com.rackspacecloud.blueflood.dw.query.types.Metric;
import com.rackspacecloud.blueflood.dw.query.types.Paging;
import com.rackspacecloud.blueflood.dw.query.types.SingleMetricResponse;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("/v2.0/{tenantId}/view/series/{metricName}")
@Produces(MediaType.APPLICATION_JSON)
public class SingleQueryResource {
    
    @GET
    @Timed
    public SingleMetricResponse getMetrics(
            @PathParam("tenantId") String tenantId,
            @PathParam("metricName") String metricName,
            @QueryParam("from") long from,
            @QueryParam("to") long to,
            @QueryParam("points") long points,
            @QueryParam("select") List<String> select,
            @QueryParam("limit") int limit,
            @QueryParam("marker") String marker ) {
        
        Metric m0 = new Metric();
        m0.setName("aaaa.bbb.ccc.1");
        m0.setTimestamp(0);
        
        Metric m1 = new Metric();
        m1.setName("aaaa.bbb.ccc.2");
        m1.setTimestamp(1);
        
        Paging paging = new Paging(50, 2, null);
        
        SingleMetricResponse response = new SingleMetricResponse();
        response.setMetrics(new Metric[] {m0, m1});
        response.setPaging(paging);
        
        return response;
    }
}
