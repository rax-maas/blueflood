package com.rackspacecloud.blueflood.dw.query.types;

import com.rackspacecloud.blueflood.dw.ingest.SimpleResponse;
import com.rackspacecloud.blueflood.types.AbstractRollupStat;
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.types.CounterRollup;
import com.rackspacecloud.blueflood.types.GaugeRollup;
import com.rackspacecloud.blueflood.types.Rollup;
import com.rackspacecloud.blueflood.types.SetRollup;
import com.rackspacecloud.blueflood.types.SimpleNumber;
import com.rackspacecloud.blueflood.types.TimerRollup;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Marshal {
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
                .entity(new SimpleResponse("No representation for this data type: " + cls.getName()))
                .build());
        }
    }
    
    private static Number coerceNumber(AbstractRollupStat stat) {
        if (stat.isFloatingPoint())
            return stat.toDouble();
        else
            return stat.toLong();
    }
    
    public static Metric marshalForOutput(Object dataPoint, Class<? extends Rollup> dataClass) throws WebApplicationException {
        if (dataClass.equals(SimpleNumber.class)) {
            Numeric m = new Numeric();
            
            m.setValue(((SimpleNumber)dataPoint).getValue());
            
            return m;
            
        } else if (dataClass.equals(BasicRollup.class)) {
            BasicRollupMetric m = new BasicRollupMetric();
            BasicRollup br = (BasicRollup)dataPoint;
            
            // populate fields
            m.setVariance(coerceNumber(br.getVariance()));
            m.setAverage(coerceNumber(br.getAverage()));
            m.setCount(br.getCount());
            m.setMax(coerceNumber(br.getMaxValue()));
            m.setMin(coerceNumber(br.getMinValue()));
            
            return m;
            
        } else if (dataClass.equals(GaugeRollup.class)) {
            GaugeMetric m = new GaugeMetric();
            GaugeRollup gr = (GaugeRollup)dataPoint;
            
            // populate fields
            m.setVariance(coerceNumber(gr.getVariance()));
            m.setAverage(coerceNumber(gr.getAverage()));
            m.setCount(gr.getCount());
            m.setMax(coerceNumber(gr.getMaxValue()));
            m.setMin(coerceNumber(gr.getMinValue()));
            m.setLatest(gr.getLatestNumericValue());
            
            return m;
            
        } else if (dataClass.equals(CounterRollup.class)) {
            CounterMetric m = new CounterMetric();
            CounterRollup cr = (CounterRollup)dataPoint;
            
            // populate fields.
            m.setCount(cr.getCount());
            m.setRate(cr.getRate());
            m.setSamples(cr.getSampleCount());
            
            return m;
            
        } else if (dataClass.equals(SetRollup.class)) {
            SetMetric m = new SetMetric();
            SetRollup sr = (SetRollup)dataPoint;
            
            // populate fields
            m.setCount(sr.getCount());
            
            return m;
            
        } else if (dataClass.equals(TimerRollup.class)) {
            TimerMetric m = new TimerMetric();
            TimerRollup tr = (TimerRollup)dataPoint;
            
            // populate fields
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
            
            return m;
        } else {
            throw new WebApplicationException(Response
                .status(Response.Status.BAD_REQUEST)
                .entity(new SimpleResponse(String.format("Cannot marshal for output: %s as %s", dataPoint.getClass().getName(), dataClass.getName())))
                .build());
        }
    }
}
