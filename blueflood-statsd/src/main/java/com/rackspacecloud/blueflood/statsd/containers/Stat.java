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

import java.util.regex.Pattern;

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
    // internal metrics are speshul.
    private static final Pattern[] InternalMetrics = new Pattern[] {
            Pattern.compile("stats_counts\\.\\w+\\.bad_lines_seen"),
            Pattern.compile("stats_counts\\.\\w+\\.packets_received"),
            
            Pattern.compile("stats\\.\\w+\\.bad_lines_seen"),
            Pattern.compile("stats\\.\\w+\\.packets_received"),
            Pattern.compile("stats\\.\\w+\\.processing_time"),
            Pattern.compile("stats\\.\\w+\\.graphiteStats\\.\\w+"),
            
            Pattern.compile("\\w+\\.numStats"),
    };
    
    private final Label label; // lots of things encapsulated here.
    private final Number value;
    private final long timestamp; // UNIX epoch time, bro. (is in seconds).
    
    
    public Stat(String label, Number value, long timestamp) {
        this.label = new Label(label);
        this.value = value;
        this.timestamp = timestamp;
    }
    
    public String getName() { return label.name; }
    public Locator getLocator() { return label.locator; }
    public long getTimestamp() { return timestamp; }
    public Number getValue() { return value; }
    public StatType getType() { return label.type; }
    public Label getLabel() { return label; }
    
    public boolean isValid() {
        return label.locator != null;
    }
    
    public String toString() {
        return Conversions.asLine(this);
    }
    
    public static class Parser {
            
        public static Number grokValue(String valuePart) {
            if (valuePart.indexOf(".") < 0)
                return Long.parseLong(valuePart);
            else
                return Double.parseDouble(valuePart);
        }
        
        public static boolean isInternal(String label) {
            for (Pattern p : InternalMetrics)
                if (p.matcher(label).matches())
                    return true;
            return false;
        }
        
        public static StatType computeType(String label) {
            if (label.startsWith("stats.timers."))
                return StatType.TIMER;
            else if (label.startsWith("stats.gauges."))
                return StatType.GAUGE;
            else if (label.startsWith("stats.sets."))
                return StatType.SET;
            else if (label.startsWith("stats_counts."))
                return StatType.COUNTER;
            else
                return StatType.UNKNOWN;
        }
        
        public static NamedLocator computeLocator(String label, boolean isInternal, StatType type) {
            String[] parts = label.split("\\.", -1);
            
            String tenantId = null;
            String[] components;
            String name = null;
            
            if (isInternal) {
                if (parts.length == 2 && "numStats".equals(parts[1])) {
                    tenantId = parts[0];
                    components = new String[] { "numStats" };
                } else {
                    tenantId = parts[1];
                    components = Stat.Parser.remove(parts, 1, 1);
                }
            } else {
                switch (type) {
                    case COUNTER:
                        tenantId = parts[1];
                        parts = Stat.Parser.remove(parts, 1, 1);
                        components = Stat.Parser.shiftLeft(parts, 1);
                        break;
                    case UNKNOWN:
                        return null; // no idea how to handle this.
                    case TIMER:
                        name = parts[parts.length - 1];
                        parts = Stat.Parser.shiftRight(parts, 1);
                    case SET:
                    case GAUGE:
                    default:
                        tenantId = parts[2];
                        components = Stat.Parser.shiftLeft(parts, 3);
                        
                        
                        break;
                }
            }
            
            return new NamedLocator(Locator.createLocatorFromPathComponents(tenantId, components), name);
            
        }
        
        /** does a left shift of the array */
        public static String[] shiftLeft(String[] s, int num) {
            return Parser.shift(s, num, true);
        }
        
        public static String[] shiftRight(String[] s, int num) {
            return Parser.shift(s, num, false);
        }
        
        private static String[] shift(String[] s, int num, boolean isLeft) {
            String[] n = new String[s.length - num];
            for (int i = 0; i < s.length - num; i++)
                n[i] = s[i + (isLeft ? num : 0)];
            return n;
        }
        
        public static String[] remove(String[] s, int index, int length) {
            String[] n = new String[s.length - length];
            int npos = 0;
            for (int i = 0; i < s.length; i++) {
                if (i < index)
                    n[npos++] = s[i];
                else if (i < index + length)
                    continue;
                else
                    n[npos++] = s[i];
            }
            return n;
        }
    }
    
    private static class Label {
        private final String original;
        private final boolean internal;
        private final StatType type;
        private final Locator locator; // this might be null!
        private final String name;
        
        Label(String s) {
            this.original = s;
            this.internal = Stat.Parser.isInternal(s);
            this.type = Stat.Parser.computeType(s);
            NamedLocator namedLocator = Stat.Parser.computeLocator(s, this.internal, this.type);
            if (namedLocator != null) {
                this.locator = namedLocator.locator;
                this.name = namedLocator.name;
            } else {
                this.locator = null;
                this.name = null;
            }
        }
        
        public String toString() {
            return original;
        }
    }
    
    // aggregator.
    private static class NamedLocator {
        private final Locator locator;
        private final String name;
        
        NamedLocator(Locator locator, String name) {
            this.locator = locator;
            this.name = name;
        }
    }
}
