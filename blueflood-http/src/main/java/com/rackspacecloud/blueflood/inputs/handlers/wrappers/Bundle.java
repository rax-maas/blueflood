/*
 * Copyright 2014 Rackspace
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

package com.rackspacecloud.blueflood.inputs.handlers.wrappers;

import com.rackspacecloud.blueflood.types.BluefloodEnum;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Using nested classes for now. Expect this to be cleaned up.
public class Bundle {
    private String tenantId;
    private long timestamp; // millis since epoch.
    
    // this field is optional
    private long flushInterval = 0;
    
    private Gauge[] gauges;
    private Counter[] counters;
    private Timer[] timers;
    private Set[] sets;
    private BluefloodEnum[] enums;
    
    private Map<String, Object> metadata;
    
    public String toString() {
        return String.format("%s (%d)", tenantId, timestamp);
    }
    
    public String getTenantId() { return tenantId; }
    
    // seconds since epoch.
    public long getTimestamp() { return timestamp; }
    
    public long getFlushIntervalMillis() { return flushInterval; }
    
    public Collection<Gauge> getGauges() { return safeAsList(gauges); }
    public Collection<Counter> getCounters() { return safeAsList(counters); }
    public Collection<Timer> getTimers() { return safeAsList(timers); }
    public Collection<Set> getSets() { return safeAsList(sets); }
    public Collection<BluefloodEnum> getEnums() { return safeAsList(enums);}

    public static class Gauge {
        private String name;
        private Number value;
        
        public String getName() { return name; }
        public Number getValue() { return value; }
    }
    
    public static class Counter {
        private String name;
        private Number value;
        private Number rate;
        
        public String getName() { return name; }
        public Number getValue() { return value; }
        public Number getRate() { return rate; }
    }
    
    public static class Set {
        private String name;
        private String[] values;
        
        public String getName() { return name; }
        public String[] getValues() { return Arrays.copyOf(values, values.length, String[].class); };
    }
    
    public static class Timer {
        private String name;
        private Number count;
        private Number rate;
        private Number min;
        private Number max;
        private Number sum;
        private Number avg;
        private Number median;
        private Number std;
        private Map<String, Percentile> percentiles;
        private Map<String, Number> histogram;
        
        public String getName() { return name; }
        public Number getCount() { return count; }
        public Number getRate() { return rate; }
        public Number getMin() { return min; }
        public Number getMax() { return max; }
        public Number getSum() { return sum; }
        public Number getAvg() { return avg; }
        public Number getMedian() { return median; }
        public Number getStd() { return std; }
        public Map<String, Percentile> getPercentiles() { return safeUnmodifiableMap(percentiles); }
        public Map<String, Number> getHistogram() { return safeUnmodifiableMap(histogram); }
    }
    
    public static class Percentile {
        private Number avg;
        private Number max;
        private Number sum;
        
        public Number getAvg() { return avg; }
        public Number getMax() { return max; }
        public Number getSum() { return sum; }
    }

    //@SafeVarargs (1.7 only doge)
    public static <T> List<T> safeAsList(final T... a) {
        if (a == null)
            return new java.util.ArrayList<T>();
        else
            return new ArrayList<T>(a);
    }
    
    public static <K,V> Map<K,V> safeUnmodifiableMap(Map<? extends K, ? extends V> m) {
        if (m == null)
            return Collections.unmodifiableMap(new HashMap<K, V>());
        else
            return Collections.unmodifiableMap(m);
    }
    
    private static class ArrayList<E> extends AbstractList<E> {
        private final E[] array;
        
        public ArrayList(E[] array) {
            this.array = array;
        }
        
        @Override
        public E get(int index) {
            if (index < 0 || index > array.length-1)
                throw new ArrayIndexOutOfBoundsException("Invalid array offset: " + index);
            return array[index];
        }

        @Override
        public int size() {
            return array.length;
        }
    }
}
