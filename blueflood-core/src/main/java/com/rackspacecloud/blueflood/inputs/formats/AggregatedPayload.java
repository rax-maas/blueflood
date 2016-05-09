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

import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.*;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.util.*;

// Using nested classes for now. Expect this to be cleaned up.
public class AggregatedPayload {

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
