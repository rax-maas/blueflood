package com.rackspacecloud.blueflood.dw.query;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Joiner;
import com.rackspacecloud.blueflood.dw.ingest.SimpleResponse;
import com.rackspacecloud.blueflood.dw.query.types.Marshal;
import com.rackspacecloud.blueflood.dw.query.types.Metric;
import com.rackspacecloud.blueflood.dw.query.types.Paging;
import com.rackspacecloud.blueflood.dw.query.types.SingleMetricResponse;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.types.CounterRollup;
import com.rackspacecloud.blueflood.types.GaugeRollup;
import com.rackspacecloud.blueflood.types.Points;
import com.rackspacecloud.blueflood.types.Rollup;
import com.rackspacecloud.blueflood.types.RollupType;
import com.rackspacecloud.blueflood.types.SetRollup;
import com.rackspacecloud.blueflood.types.SimpleNumber;
import com.rackspacecloud.blueflood.types.TimerRollup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Path("/v2.0/{tenantId}/view")
@Produces(MediaType.APPLICATION_JSON)
public class QueryResource {
    private static final Logger log = LoggerFactory.getLogger(QueryResource.class);
    
    private RollupHandler handler = new RollupHandler();
    
    @GET
    @Timed
    @Path("series/{metricName}")
    public SingleMetricResponse getMetrics(
            @PathParam("tenantId") String tenantId,
            @PathParam("metricName") String metricName,
            @QueryParam("from") long from,
            @QueryParam("to") long to,
            @QueryParam("points") int points,
            @QueryParam("select") List<String> select,
            @QueryParam("limit") int limit,
            @QueryParam("marker") String marker ) {
        
        List<Metric> returnMetrics = new ArrayList<Metric>();
        MetricData metricData = handler.byPoints(tenantId, metricName, from, to, points);
        Points data = metricData.getData();
        RollupType rollupType = RollupType.NOT_A_ROLLUP;
        
        if (data.getPoints().size() > 0) {
            Class<? extends Rollup> dataClass = data.getDataClass();
            Class metricClass = Marshal.getMetricClassFor(dataClass);
            String jsonFilterId = Joiner.on(",").join(Marshal.getFilterFields(metricClass, select));
            
            if (dataClass.equals(BasicRollup.class))
                rollupType = RollupType.BF_BASIC;
            else if (dataClass.equals(SimpleNumber.class))
                rollupType = RollupType.BF_BASIC;
            else if (dataClass.equals(SetRollup.class))
                rollupType = RollupType.SET;
            else if (dataClass.equals(CounterRollup.class))
                rollupType = RollupType.COUNTER;
            else if (dataClass.equals(GaugeRollup.class))
                rollupType = RollupType.GAUGE;
            else if (dataClass.equals(TimerRollup.class))
                rollupType = RollupType.TIMER;
            
            
            Map<Long, Points.Point<?>> map = data.getPoints();
            for (Map.Entry<Long, Points.Point<?>> entry : map.entrySet()) {
                long stamp = entry.getKey();
                Points.Point<?> point = entry.getValue();
                Object dataPoint = point.getData();
                Metric m = Marshal.marshalForOutput(dataPoint, dataClass);
                
                // standard fields
                m.setName(metricName);
                m.setTimestamp(stamp);
                
                // set the filter this tells the configured MetricFilterProvider which fields to keep for output.
                m.setJsonFilter(jsonFilterId);
                
                returnMetrics.add(m);
            }
        }
        
        if (rollupType == null) {
            rollupType = RollupType.NOT_A_ROLLUP;
        }
        
        // todo: cache and paging later.

        Paging paging = new Paging(0, 0, null);
        SingleMetricResponse response = new SingleMetricResponse();
        response.setType(rollupType.name().toLowerCase());
        response.setMetrics(returnMetrics.toArray(new Metric[returnMetrics.size()]));
        response.setPaging(paging);
        
        return response;
    }
    
    @POST
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("multiseries")
    public MultiMetricResponse getMetrics(
            @PathParam("tenantId") String tenantId,
            @QueryParam("from") long from,
            @QueryParam("to") long to,
            @QueryParam("points") int points,
            @QueryParam("select") List<String> select,
            @QueryParam("limit") int limit,
            @QueryParam("marker") String marker,
            List<String> metricNames
    ) {
        
        // todo: multithread this sucker.
        MultiMetricResponse response = new MultiMetricResponse();
        for (String metricName : metricNames) {
            SingleMetricResponse singleResponse = getMetrics(tenantId, metricName, from, to, points, select, limit, marker);
            response.addResponse(metricName, singleResponse);
        }
        
        return response;
    }
    
    private class RollupHandler extends com.rackspacecloud.blueflood.outputs.handlers.RollupHandler {
        public MetricData byPoints(String tenantId, String metric, long from, long to, int points) {
            Granularity g = Granularity.granularityFromPointsInInterval(from, to, points);
            return this.getRollupByGranularity(tenantId, metric, from, to, g);
        }
        
        public MetricData byResolution(String tenantId, String metric, long from, long to, Granularity granularity) {
            return this.getRollupByGranularity(tenantId, metric, from, to, granularity);
        }
    }
}
