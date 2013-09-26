package com.rackspacecloud.blueflood.statsd;

import com.rackspacecloud.blueflood.types.Metric;

/**
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
 *  
 * 
 *  
 *  
 * 
 */
public class Stat {
    private String label;
    private Number value;
    private long timestamp; // UNIX epoch time, bro.  (is in seconds).
    
    private Stat(String label, Number value, long timestamp) {
        this.label = label;
        this.value = value;
        this.timestamp = timestamp;
    }
    
    public String getLabel() { return label; }
    public long getTimestamp() { return timestamp; }
    public Number getValue() { return value; }
    
    public <T extends Number> Number getValueAs(Class<T> type) {
        return value;
    }
    
    public String toString() {
        return Stat.asLine(this);
    }
    
    public static Stat fromLine(CharSequence str) {
        String[] parts = str.toString().split(" ", -1);
        return new Stat(parts[0], Stat.grokValue(parts[1]), Long.parseLong(parts[2]));
    }
    
    public static Number grokValue(String s) {
        if (s.indexOf(".") < 0)
            return Long.parseLong(s);
        else
            return Double.parseDouble(s);
    }
    
    public static String asLine(Stat stat) {
        return String.format("%s %s %d", stat.label, stat.value, stat.timestamp);
    }
    
    public static Metric asMetric(Stat stat) {
        String label = stat.getLabel();
        String[] labelParts = label.split("\\.", -1);
        
        if (label.startsWith("stats.timers")) {
            // process as timer.
            // todo: this is a pre-aggregated rollup. we don't know how to handle this yet.
            return null;
        } else if (label.startsWith("stats.gauges.")) {
            // process as gauge
            return null;
        } else if (label.startsWith("stats.sets.")) {
            // process as set
            return null;
        } else if (label.startsWith("stats_counts.")) {
            // process as counter
            return null;
        } else {
            // let's discard for now.
            return null;
        }
    }
    
    
}
