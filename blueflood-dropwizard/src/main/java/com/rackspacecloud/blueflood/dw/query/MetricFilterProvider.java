package com.rackspacecloud.blueflood.dw.query;

import com.fasterxml.jackson.databind.ser.BeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.PropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.rackspacecloud.blueflood.dw.query.types.Metric;
import com.rackspacecloud.blueflood.dw.query.types.OutputFilters;

import java.util.HashMap;
import java.util.Map;

/**
 * Dynamicaly filter values based on a filter spec attached to the ojbect (not via annotation).
 * todo: verify the concurrency properties of SimpleBeanPropertyFilter.
 */
public class MetricFilterProvider extends FilterProvider {
    
    private Map<String, PropertyFilter> filters = new HashMap<String, PropertyFilter>();
    
    @Override
    public BeanPropertyFilter findFilter(Object filterId) {
        // this method is deprecated. Do not return anything!
        return null;
    }

    @Override
    public PropertyFilter findPropertyFilter(Object filterId, Object valueToFilter) {
        if (filterId.equals("dynamic") && valueToFilter instanceof Metric) {
            Metric m = (Metric)valueToFilter;
            if (m.getJsonFilter() == null || m.getJsonFilter().equals("ALL")) {
                return super.findPropertyFilter(filterId, valueToFilter);
            }
            
            return getOrMakeFilter(m.getJsonFilter());
            
        } else {
            return super.findPropertyFilter(filterId, valueToFilter);
        }
    }
    
    private PropertyFilter getOrMakeFilter(String filterSpec) {
        PropertyFilter filter = filters.get(filterSpec);
        if (filter == null) {
            synchronized (filters) {
                filter = SimpleBeanPropertyFilter.filterOutAllExcept(filterSpec.split(",", -1));
                filters.put(filterSpec, filter);
            }
        }
        return filter;
    }
}
