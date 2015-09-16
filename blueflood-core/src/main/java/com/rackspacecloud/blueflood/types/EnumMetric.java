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

package com.rackspacecloud.blueflood.types;

import com.rackspacecloud.blueflood.exceptions.InvalidDataException;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class EnumMetric implements IMetric {
    private static final Logger log = LoggerFactory.getLogger(EnumMetric.class);

    private final Locator locator;
    private Object metricValue;
    private final long collectionTime;
    private int ttlInSeconds;
    private DataType dataType;
    private final ArrayList<String> enumValues;

    public EnumMetric(Locator locator, Object metricValue, long collectionTime, TimeValue ttl, ArrayList<String> enumValues) {
        this.locator = locator;
        this.metricValue = metricValue;
        if (collectionTime < 0) {
            throw new InvalidDataException("collection time must be greater than zero");
        }
        this.collectionTime = collectionTime;
        this.dataType = DataType.getMetricType(metricValue);
        this.enumValues = enumValues;
        setTtl(ttl);
    }

    public Locator getLocator() {
        return locator;
    }

    public Object getMetricValue() {
        return metricValue;
    }

    public DataType getDataType() {
        return dataType;
    }

    public int getTtlInSeconds() {
        return ttlInSeconds;
    }

    public long getCollectionTime() {
        return collectionTime;
    }

    public ArrayList<String> getEnumValues() {
        return enumValues;
    }

    public void setTtl(TimeValue ttl) {
        if (!isValidTTL(ttl.toSeconds())) {
            throw new InvalidDataException("TTL supplied for metric is invalid. Required: 0 < ttl < " + Integer.MAX_VALUE +
                    ", provided: " + ttl.toSeconds());
        }

        ttlInSeconds = (int) ttl.toSeconds();
    }

    public void setTtlInSeconds(int ttlInSeconds) {
        if (!isValidTTL(ttlInSeconds)) {
            throw new InvalidDataException("TTL supplied for metric is invalid. Required: 0 < ttl < " + Integer.MAX_VALUE +
                    ", provided: " + ttlInSeconds);
        }

        this.ttlInSeconds = ttlInSeconds;
    }

    public RollupType getRollupType() {
        return RollupType.BF_BASIC;
    }

    @Override
    public String toString() {
        String enumValuesString = "";
        if (enumValues != null) {
            for (String enumValue : enumValues) {
                if (enumValuesString != "") enumValuesString += ",";
                enumValuesString += enumValue.replace(",", "\\,");
            }
        }
        return String.format("%s:%s:%s:%s:%s", locator.toString(), metricValue, dataType, ttlInSeconds, enumValuesString);
    }

    private boolean isValidTTL(long ttlInSeconds) {
        return (ttlInSeconds < Integer.MAX_VALUE && ttlInSeconds > 0);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof EnumMetric)) {
            return false;
        }
        EnumMetric other = (EnumMetric) o;

        ArrayList<String> otherEnumValues = other.getEnumValues();
        boolean enumValuesEqual = true;
        if ((enumValues != null) || (otherEnumValues != null)){
            if (enumValues != null) {
                enumValuesEqual = enumValues.equals(otherEnumValues);
            }
            else {
                enumValuesEqual = otherEnumValues.equals(enumValues);
            }
        }

        if (locator.equals(other.getLocator()) &&
                collectionTime == other.getCollectionTime() &&
                ttlInSeconds == other.getTtlInSeconds() &&
                dataType.equals(other.getDataType()) &&
                enumValuesEqual) {
            return true;
        }
        return false;
    }
}
