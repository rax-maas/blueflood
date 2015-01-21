/*
 * Copyright 2015 Rackspace
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

import java.util.*;

public class MetricsBundle {

    // this field is optional
    private long flushInterval = 0;

    private Gauge[] gauges;
    private Counter[] counters;
    private Timer[] timers;
    private Set[] sets;

    private Map<String, Object> metadata;

    public long getFlushIntervalMillis() { return flushInterval; }

    public Collection<Gauge> getGauges() { return safeAsList(gauges); }
    public Collection<Counter> getCounters() { return safeAsList(counters); }
    public Collection<Timer> getTimers() { return safeAsList(timers); }
    public Collection<Set> getSets() { return safeAsList(sets); }

    public static class Gauge {
        private String metricName;
        private Number metricValue;
        private long collectionTime;
        private int ttlInSeconds;
        private String unit;

        public String getMetricName() { return metricName; }
        public Number getMetricValue() { return metricValue; }
        public long getCollectionTime() { return collectionTime; }
        public int getTtlInSeconds() { return ttlInSeconds; }
        public String getUnit() { return unit; }
    }

    public static class Counter {
        private String name;
        private Number value;
        private Number rate;
        private long collectionTime;
        private int ttlInSeconds;

        public String getName() { return name; }
        public Number getValue() { return value; }
        public Number getRate() { return rate; }
        public long getCollectionTime() { return collectionTime; }
        public int getTtlInSeconds() { return ttlInSeconds; }
    }

    public static class Set {
        private String name;
        private String[] values;
        private long collectionTime;
        private int ttlInSeconds;

        public String getName() { return name; }
        public String[] getValues() { return Arrays.copyOf(values, values.length, String[].class); };
        public long getCollectionTime() { return collectionTime; }
        public int getTtlInSeconds() { return ttlInSeconds; }
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
        private long collectionTime;
        private int ttlInSeconds;

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
        public long getCollectionTime() { return collectionTime; }
        public int getTtlInSeconds() { return ttlInSeconds; }
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
