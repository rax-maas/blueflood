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

package com.rackspacecloud.blueflood.inputs.formats;

import com.google.gson.Gson;
import com.rackspacecloud.blueflood.io.Instrumentation;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.*;

import java.util.*;

// Using nested classes for now. Expect this to be cleaned up.
public class AggregatedPayload {

    private static final long TRACKER_DELAYED_METRICS_MILLIS = Configuration.getInstance().getLongProperty(CoreConfig.TRACKER_DELAYED_METRICS_MILLIS);
    private static final long MAX_AGE_ALLOWED = Configuration.getInstance().getLongProperty(CoreConfig.ROLLUP_DELAY_MILLIS);
    private static final long SHORT_DELAY = Configuration.getInstance().getLongProperty(CoreConfig.SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS);
    private final long BEFORE_CURRENT_COLLECTIONTIME_MS = Configuration.getInstance().getLongProperty( CoreConfig.BEFORE_CURRENT_COLLECTIONTIME_MS );
    private final long AFTER_CURRENT_COLLECTIONTIME_MS = Configuration.getInstance().getLongProperty( CoreConfig.AFTER_CURRENT_COLLECTIONTIME_MS );

    private String tenantId;
    private long timestamp; // millis since epoch.

    // this field is optional
    private long flushInterval = 0;
    
    private BluefloodGauge[] gauges;
    private BluefloodCounter[] counters;
    private BluefloodTimer[] timers;
    private BluefloodSet[] sets;
    private BluefloodEnum[] enums;
    
    private Map<String, Object> metadata;

    public static AggregatedPayload create(String json) {
        AggregatedPayload payload = new Gson().fromJson(json, AggregatedPayload.class);
        return payload;
    }
    
    public String toString() {
        return String.format("%s (%d)", tenantId, timestamp);
    }
    
    public String getTenantId() { return tenantId; }
    
    // seconds since epoch.
    public long getTimestamp() { return timestamp; }
    
    public long getFlushIntervalMillis() { return flushInterval; }
    
    public Collection<BluefloodGauge> getGauges() { return safeAsList(gauges); }
    public Collection<BluefloodCounter> getCounters() { return safeAsList(counters); }
    public Collection<BluefloodTimer> getTimers() { return safeAsList(timers); }
    public Collection<BluefloodSet> getSets() { return safeAsList(sets); }
    public Collection<BluefloodEnum> getEnums() { return safeAsList(enums); }

    public List<String> getValidationErrors() {
        List<String> errors = new java.util.ArrayList<String>();

        long currentTime = System.currentTimeMillis();
        if ( timestamp > currentTime + AFTER_CURRENT_COLLECTIONTIME_MS ) {
            // timestamp is too far in the future
            errors.add( "'timestamp' '" + timestamp + "' is more than '" + AFTER_CURRENT_COLLECTIONTIME_MS + "' milliseconds into the future." );
        } else if ( timestamp < currentTime - BEFORE_CURRENT_COLLECTIONTIME_MS ) {
            // timestamp is too far in the past
            errors.add( "'timestamp' '" + timestamp + "' is more than '" + BEFORE_CURRENT_COLLECTIONTIME_MS + "' milliseconds into the past." );
        }

        return errors;
    }

    /**
     * Determines if the metrics represented in this {@link AggregatedPayload}
     * has collection timestamp that we consider "late" or "delayed".
     *
     * @param ingestTime
     * @return
     */
    public boolean hasDelayedMetrics(long ingestTime) {
        return getDelayTime(ingestTime) > TRACKER_DELAYED_METRICS_MILLIS;
    }

    /**
     * Calculates how many milliseconds is the collection timestamp of
     * this {@link AggregatedPayload} delayed by.
     *
     * @param ingestTime  ingest timestamp in milliseconds
     * @return
     */
    public long getDelayTime(long ingestTime) {
        return ingestTime - timestamp;
    }

    /**
     * Marks/instruments our internal metrics that we have received
     * short or delayed metrics
     *
     * @param ingestTime
     * @return
     */
    public void markDelayMetricsReceived(long ingestTime) {
        long delay = getDelayTime(ingestTime);
        if ( delay > MAX_AGE_ALLOWED ) {
            if ( delay <= SHORT_DELAY ) {
                Instrumentation.markMetricsWithShortDelayReceived();
            } else {
                Instrumentation.markMetricsWithLongDelayReceived();
            }
        }
    }

    public List<String> getAllMetricNames() {
        List<String> metricNames = new java.util.ArrayList<String>();
        if ( gauges != null && gauges.length > 0) {
            for (int index=0; index<gauges.length; index++) {
                metricNames.add(gauges[index].getName());
            }
        }

        if ( counters != null && counters.length > 0) {
            for (int index=0; index<counters.length; index++) {
                metricNames.add(counters[index].getName());
            }
        }

        if ( timers != null && timers.length > 0) {
            for (int index=0; index<timers.length; index++) {
                metricNames.add(timers[index].getName());
            }
        }

        if ( sets != null && sets.length > 0) {
            for (int index = 0; index < sets.length; index++) {
                metricNames.add(sets[index].getName());
            }
        }

        if ( enums != null && enums.length > 0) {
            for (int index=0; index<enums.length; index++) {
                metricNames.add(enums[index].getName());
            }
        }
        return metricNames;
    }

    //@SafeVarargs (1.7 only doge)
    public static <T> List<T> safeAsList(final T... a) {
        if (a == null)
            return new java.util.ArrayList<T>();
        else
            return new ArrayList<T>(a);
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
