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
package com.rackspacecloud.blueflood.inputs.formats;

import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;

import java.util.List;

// Jackson compatible class. Jackson uses reflection to call these methods and so they have to match JSON keys.
public class JSONMetric {

    final long BEFORE_CURRENT_COLLECTIONTIME_MS = Configuration.getInstance().getLongProperty( CoreConfig.BEFORE_CURRENT_COLLECTIONTIME_MS );
    final long AFTER_CURRENT_COLLECTIONTIME_MS = Configuration.getInstance().getLongProperty( CoreConfig.AFTER_CURRENT_COLLECTIONTIME_MS );

    private String metricName;
    private Object metricValue;
    private long collectionTime;
    private int ttlInSeconds;
    private String unit;

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public Object getMetricValue() {
        return metricValue;
    }

    public void setMetricValue(Object metricValue) {
        this.metricValue = metricValue;
    }

    public long getCollectionTime() {
        return collectionTime;
    }

    public void setCollectionTime(long collectionTime) {
        this.collectionTime = collectionTime;
    }

    public int getTtlInSeconds() {
        return this.ttlInSeconds;
    }

    public void setTtlInSeconds(int ttlInSeconds) {
        this.ttlInSeconds = ttlInSeconds;
    }

    public List<String> getValidationErrors() {
        List<String> errors = new java.util.ArrayList<String>();

        long currentTime = System.currentTimeMillis();
        if ( collectionTime < currentTime - BEFORE_CURRENT_COLLECTIONTIME_MS ) {
            // collectionTime is too far in the past
            errors.add( "'" + metricName + "': 'collectionTime' '" + collectionTime + "' is more than '" + BEFORE_CURRENT_COLLECTIONTIME_MS + "' milliseconds into the past." );
        } else if ( collectionTime > currentTime + AFTER_CURRENT_COLLECTIONTIME_MS ) {
            // collectionTime is too far in the future
            errors.add( "'" + metricName + "': 'collectionTime' '" + collectionTime + "' is more than '" + AFTER_CURRENT_COLLECTIONTIME_MS + "' milliseconds into the future." );
        }

        return errors;
    }

}