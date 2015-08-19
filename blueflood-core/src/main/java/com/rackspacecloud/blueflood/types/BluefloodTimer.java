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

package com.rackspacecloud.blueflood.types;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class BluefloodTimer {
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
