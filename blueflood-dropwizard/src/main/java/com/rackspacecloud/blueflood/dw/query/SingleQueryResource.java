package com.rackspacecloud.blueflood.dw.query;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Joiner;
import com.rackspacecloud.blueflood.dw.ingest.SimpleResponse;
import com.rackspacecloud.blueflood.dw.query.types.BasicRollupMetric;
import com.rackspacecloud.blueflood.dw.query.types.CounterMetric;
import com.rackspacecloud.blueflood.dw.query.types.GaugeMetric;
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
    
    public static List<String> getFilterFields(Class type, List<String> specifiedSelectFields) {
        
        // if no fields were specified, use the defaults.
        if (specifiedSelectFields == null || specifiedSelectFields.size() == 0) {
            return OutputFilters.standardFilterFor(type);
        }
        
        // determine what fields we want to filter for;
        List<String> selectFilter = new ArrayList<String>();
        for (String s : specifiedSelectFields) {
            for (String filter : s.split(",", -1)) {
                selectFilter.add(filter);
            }
        }
        Collections.sort(selectFilter);
        return selectFilter;
    }
    
    public static Class getMetricClassFor(Class cls) throws WebApplicationException {
        if (cls.equals(SimpleNumber.class)) {
            return Numeric.class;
        } else if (cls.equals(BasicRollup.class)) {
            return BasicRollupMetric.class;
        } else if (cls.equals(GaugeRollup.class)) {
            return GaugeMetric.class;
        } else if (cls.equals(CounterRollup.class)) {
            return CounterMetric.class;
        } else if (cls.equals(SetRollup.class)) {
            return SetMetric.class;
        } else if (cls.equals(TimerRollup.class)) {
            return TimerMetric.class;
        } else {
            throw new WebApplicationException(Response
                .status(Response.Status.BAD_REQUEST)
                .entity(new SimpleResponse("No representation for this data type: "+ cls.getName()))
                .build());
        }
    }
    
    private static Number coerceNumber(AbstractRollupStat stat) {
        if (stat.isFloatingPoint())
            return stat.toDouble();
        else
            return stat.toLong();
    }
    
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
            Class dataClass = data.getDataClass();
            Class metricClass = getMetricClassFor(dataClass);
            String jsonFilterId = Joiner.on(",").join(getFilterFields(metricClass, select));
            
            Map<Long, Points.Point<?>> map = data.getPoints();
            for (Map.Entry<Long, Points.Point<?>> entry : map.entrySet()) {
                long stamp = entry.getKey();
                Points.Point<?> point = entry.getValue();
                Object dataPoint = point.getData();
                //log.info("{} {}", dataClass.getSimpleName(), dataPoint.getClass().getSimpleName());
                
                if (dataClass.equals(SimpleNumber.class)) {
                    Numeric m = new Numeric();
                    
                    // set the filter
                    m.setJsonFilter(jsonFilterId);
                    
                    // populate fields.
                    m.setName(metricName);
                    m.setTimestamp(stamp);
                    m.setValue(((SimpleNumber)dataPoint).getValue());
                    
                    returnMetrics.add(m);
                    
                } else if (dataClass.equals(BasicRollup.class)) {
                    BasicRollupMetric m = new BasicRollupMetric();
                    BasicRollup br = (BasicRollup)dataPoint;
                    m.setJsonFilter(jsonFilterId);
                    
                    // populate fields
                    m.setName(metricName);
                    m.setTimestamp(stamp);
                    m.setVariance(coerceNumber(br.getVariance()));
                    m.setAverage(coerceNumber(br.getAverage()));
                    m.setCount(br.getCount());
                    m.setMax(coerceNumber(br.getMaxValue()));
                    m.setMin(coerceNumber(br.getMinValue()));
                    
                    returnMetrics.add(m);
                    
                } else if (dataClass.equals(GaugeRollup.class)) {
                    GaugeMetric m = new GaugeMetric();
                    GaugeRollup gr = (GaugeRollup)dataPoint;
                    m.setJsonFilter(jsonFilterId);
                    
                    // populate fields
                    m.setName(metricName);
                    m.setTimestamp(stamp);
                    m.setVariance(coerceNumber(gr.getVariance()));
                    m.setAverage(coerceNumber(gr.getAverage()));
                    m.setCount(gr.getCount());
                    m.setMax(coerceNumber(gr.getMaxValue()));
                    m.setMin(coerceNumber(gr.getMinValue()));
                    m.setLatest(gr.getLatestNumericValue());
                    
                    returnMetrics.add(m);
                    
                } else if (dataClass.equals(CounterRollup.class)) {
                    CounterMetric m = new CounterMetric();
                    CounterRollup cr = (CounterRollup)dataPoint;
                    m.setJsonFilter(jsonFilterId);
                    
                    // populate fields.
                    m.setName(metricName);
                    m.setTimestamp(stamp);
                    m.setCount(cr.getCount());
                    m.setRate(cr.getRate());
                    m.setSamples(cr.getSampleCount());
                    
                    returnMetrics.add(m);
                    
                } else if (dataClass.equals(SetRollup.class)) {
                    SetMetric m = new SetMetric();
                    SetRollup sr = (SetRollup)dataPoint;
                    m.setJsonFilter(jsonFilterId);
                    
                    // populate fields
                    m.setName(metricName);
                    m.setTimestamp(stamp);
                    m.setDistinctValues(sr.getCount());
                    
                    returnMetrics.add(m);
                    
                } else if (dataClass.equals(TimerRollup.class)) {
                    TimerMetric m = new TimerMetric();
                    TimerRollup tr = (TimerRollup)dataPoint;
                    m.setJsonFilter(jsonFilterId);
                    
                    // populate fields
                    m.setName(metricName);
                    m.setTimestamp(stamp);
                    m.setVariance(coerceNumber(tr.getVariance()));
                    m.setAverage(coerceNumber(tr.getAverage()));
                    m.setCount(tr.getCount());
                    m.setMax(coerceNumber(tr.getMaxValue()));
                    m.setMin(coerceNumber(tr.getMinValue()));
                    m.setSum(tr.getSum());
                    m.setRate(tr.getRate());
                    m.setSamples(tr.getSampleCount());
                    Map<String, Number> percentiles = new HashMap<String, Number>();
                    for (Map.Entry<String, TimerRollup.Percentile> percentile : tr.getPercentiles().entrySet()) {
                        percentiles.put(percentile.getKey(), percentile.getValue().getMean());
                    }
                    m.setPercentiles(percentiles);
                    
                    returnMetrics.add(m);
                }
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
