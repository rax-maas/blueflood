/*
 * Copyright 2013 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.statsd.containers;

import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.PreaggregatedMetric;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class TypedMetricsCollection {
    
    
    private Collection<Metric> normalMetrics = new ArrayList<Metric>();
    private Collection<PreaggregatedMetric> preaggregatedMetrics = new ArrayList<PreaggregatedMetric>();
    
    public Collection<Metric> getNormalMetrics() {
        return Collections.unmodifiableCollection(normalMetrics);
    }
    
    public Collection<PreaggregatedMetric> getPreaggregatedMetrics() {
        return Collections.unmodifiableCollection(preaggregatedMetrics);
    }
    
    public void addMetric(IMetric metric) {
        if (metric instanceof Metric)
            normalMetrics.add((Metric)metric);
        else if (metric instanceof PreaggregatedMetric)
            preaggregatedMetrics.add((PreaggregatedMetric)metric);
    }
}