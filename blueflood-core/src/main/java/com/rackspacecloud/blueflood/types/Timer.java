package com.rackspacecloud.blueflood.types;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class Timer {
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

    public String getName() {
        return name;
    }

    public Number getCount() {
        return count;
    }

    public Number getRate() {
        return rate;
    }

    public Number getMin() {
        return min;
    }

    public Number getMax() {
        return max;
    }

    public Number getSum() {
        return sum;
    }

    public Number getAvg() {
        return avg;
    }

    public Number getMedian() {
        return median;
    }

    public Number getStd() {
        return std;
    }

    public Map<String, Percentile> getPercentiles() {
        return safeUnmodifiableMap(percentiles);
    }

    public Map<String, Number> getHistogram() {
        return safeUnmodifiableMap(histogram);
    }

    public static <K,V> Map<K,V> safeUnmodifiableMap(Map<? extends K, ? extends V> m) {
        if (m == null)
            return Collections.unmodifiableMap(new HashMap<K, V>());
        else
            return Collections.unmodifiableMap(m);
    }
}
