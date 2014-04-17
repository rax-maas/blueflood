package com.rackspacecloud.blueflood.dw.query.types;

import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OutputFilters {
    private static final Map<Class, List<String>> filters = new HashMap<Class, List<String>>() {{
        
        // Metric.class
        put(Metric.class, Lists.newArrayList("timestamp"));
        
        // Numeric.class
        List<String> forNumeric = Lists.newArrayList("value");
        forNumeric.addAll(get(Metric.class));
        put(Numeric.class, forNumeric);
        
        // BasicRollupMetric.class
        List<String> forBasic = Lists.newArrayList("average", "count");
        forBasic.addAll(get(Metric.class));
        put(BasicRollupMetric.class, forBasic);
        
        // GaugeRollup.class
        List<String> forGauge = Lists.newArrayList("count", "latest");
        forGauge.addAll(get(Metric.class));
        put(GaugeMetric.class, forGauge);
        
        // CounterRollup.class
        List<String> forCounter = Lists.newArrayList("count", "latest");
        forCounter.addAll(get(Metric.class));
        put(CounterMetric.class, forCounter);
        
        // SetRollup.class
        List<String> forSet = Lists.newArrayList("count");
        forSet.addAll(get(Metric.class));
        put(SetMetric.class, forSet);
        
        // TimerRollup.class
        List<String> forTimer = Lists.newArrayList("rate", "average", "count");
        forTimer.addAll(get(Metric.class));
        put(TimerMetric.class, forTimer);
    }};
    
    public static List<String> standardFilterFor(Class cls) {
        return filters.get(cls);
    }
}
