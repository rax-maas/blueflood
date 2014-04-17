package com.rackspacecloud.blueflood.dw.query;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Joiner;
import com.rackspacecloud.blueflood.dw.ingest.SimpleResponse;
import com.rackspacecloud.blueflood.dw.query.types.BasicRollupMetric;
import com.rackspacecloud.blueflood.dw.query.types.CounterMetric;
import com.rackspacecloud.blueflood.dw.query.types.GaugeMetric;
import com.rackspacecloud.blueflood.dw.query.types.Marshal;
import com.rackspacecloud.blueflood.dw.query.types.Metric;
import com.rackspacecloud.blueflood.dw.query.types.Numeric;
import com.rackspacecloud.blueflood.dw.query.types.OutputFilters;
import com.rackspacecloud.blueflood.dw.query.types.Paging;
import com.rackspacecloud.blueflood.dw.query.types.SetMetric;
import com.rackspacecloud.blueflood.dw.query.types.SingleMetricResponse;
import com.rackspacecloud.blueflood.dw.query.types.TimerMetric;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.AbstractRollupStat;
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.types.CounterRollup;
import com.rackspacecloud.blueflood.types.GaugeRollup;
import com.rackspacecloud.blueflood.types.Points;
import com.rackspacecloud.blueflood.types.Rollup;
import com.rackspacecloud.blueflood.types.SetRollup;
import com.rackspacecloud.blueflood.types.SimpleNumber;
import com.rackspacecloud.blueflood.types.TimerRollup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Path("/v2.0/{tenantId}/view/series/{metricName}")
@Produces(MediaType.APPLICATION_JSON)
public class SingleQueryResource {
    private static final Logger log = LoggerFactory.getLogger(SingleQueryResource.class);
    
    private RollupHandler handler = new RollupHandler();
    
    @GET
    @Timed
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
        
        if (data.getPoints().size() > 0) {
            Class<? extends Rollup> dataClass = data.getDataClass();
            Class metricClass = Marshal.getMetricClassFor(dataClass);
            String jsonFilterId = Joiner.on(",").join(Marshal.getFilterFields(metricClass, select));
            
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
            }
        }
        
        // todo: cache and paging later.

        Paging paging = new Paging(0, 0, null);
        SingleMetricResponse response = new SingleMetricResponse();
        response.setMetrics(returnMetrics.toArray(new Metric[returnMetrics.size()]));
        response.setPaging(paging);
        
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
