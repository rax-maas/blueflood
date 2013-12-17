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

import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.StatType;


/**
 * What if the "prefix" is the tenantId? Then all the meta stats can be converted to locators.
 * Then, we'll require that the tenantId will be the first part of the metric name tuple (after stats or whatever).
 * 
 * Stat labels will have the following formats:
 * 
 * First: meta information:
 *  // the two default stats (counters)
 *  stats.prefix.bad_lines_seen
 *  stats.prefix.packets_received
 *  stats_counts.prefix.bad_lines_seen
 *  stats_counts.prefix.packets_received
 *  // normal meta information.
 *  prefix.numStats
 *  stats.prefix.processing_time
 *  stats.prefix.graphiteStats.calculationtime
 *  stats.prefix.graphiteStats.last_exception
 *  stats.prefix.graphiteStats.last_flush
 *  stats.prefix.graphiteStats.flush_time
 *  stats.prefix.graphiteStats.flush_length <-- I've observed that this doesn't add up.
 *  
 * Counters:
 *  stats.{your counter name} <-- not sure what this is. some kind of rate or derivative.
 *  stats_counts.{your counter name} <-- current actual value.
 *  
 * Timers
 *  stats.timers.{your timer name}.mean_{percentile}
 *  stats.timers.{your timer name}.upper_{percentile}
 *  stats.timers.{your timer name}.sum_{percentile}
 *  stats.timers.{your timer name}.std
 *  stats.timers.{your timer name}.upper
 *  stats.timers.{your timer name}.lower
 *  stats.timers.{your timer name}.count
 *  stats.timers.{your timer name}.count_ps
 *  stats.timers.{your timer name}.sum
 *  stats.timers.{your timer name}.mean
 *  stats.timers.{your timer name}.median
 *  
 * Gauges
 *  stats.gauges.{your gauge name}
 * 
 * Sets
 *  stats.sets.{your set name} <-- followed by the number of unique values observed by this statsd.
 *  
 */
public class Stat {
    private final StatLabel label; // lots of things encapsulated here.
    private final Number value;
    private final long timestamp; // UNIX epoch time, bro. (is in seconds).
    
    
    public Stat(StatLabel label, Number value, long timestamp) {
        this.label = label;
        this.value = value;
        this.timestamp = timestamp;
    }
    
    public String getNameIfTimer() { return label.getName(); }
    public Locator getLocator() { return label.getLocator(); }
    public long getTimestamp() { return timestamp; }
    public Number getValue() { return value; }
    public StatType getType() { return label.getType(); }
    public StatLabel getLabel() { return label; }
    
    public boolean isValid() {
        return label.getLocator() != null;
    }
    
    public String toString() {
        return Conversions.asLine(this);
    }
}
