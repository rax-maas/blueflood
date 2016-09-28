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

import com.rackspacecloud.blueflood.inputs.constraints.EpochRange;
import com.rackspacecloud.blueflood.inputs.constraints.EpochRangeLimits;
import org.hibernate.validator.constraints.NotEmpty;
import org.hibernate.validator.constraints.Range;

// Jackson compatible class. Jackson uses reflection to call these methods and so they have to match JSON keys.
public class JSONMetric {

    final static String ERROR_MESSAGE = "xxx " + EpochRangeLimits.BEFORE_CURRENT_TIME_MS.getValue();

    @NotEmpty
    private String metricName;

    private Object metricValue;

    @EpochRange(maxPast = EpochRangeLimits.BEFORE_CURRENT_TIME_MS,
                maxFuture = EpochRangeLimits.AFTER_CURRENT_TIME_MS,
                message = "Out of bounds. Cannot be more than ${maxPast.getValue()} milliseconds into the past. Cannot be more than ${maxFuture.getValue()} milliseconds into the future")
    private long collectionTime;

    @Range(min=1, max=Integer.MAX_VALUE)
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

}